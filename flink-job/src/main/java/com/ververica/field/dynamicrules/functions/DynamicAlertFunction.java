/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.Alert;
import com.ververica.field.dynamicrules.Event;
import com.ververica.field.dynamicrules.FieldsExtractor;
import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RuleHelper;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.addToStateValuesSet;
import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

/**
 * Implements main rule evaluation and alerting logic.
 */
@Slf4j
public class DynamicAlertFunction
    extends KeyedBroadcastProcessFunction<
    String, Keyed<Event, String, Integer>, Rule, Alert> {

  private static final String COUNT = "COUNT_FLINK";
  private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

  private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;

  private transient MapState<Long, Set<Event>> windowState;
  private Meter alertMeter;

  private MapStateDescriptor<Long, Set<Event>> windowStateDescriptor =
      new MapStateDescriptor<>(
          "windowState",
          BasicTypeInfo.LONG_TYPE_INFO,
          TypeInformation.of(new TypeHint<Set<Event>>() {
          }));

  @Override
  public void open(Configuration parameters) {

    windowState = getRuntimeContext().getMapState(windowStateDescriptor);

    alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
  }

  /**
   * 处理事件流的信息
   * @param value
   * @param ctx
   * @param out
   * @throws Exception
   */
  @Override
  public void processElement(
      Keyed<Event, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out)
      throws Exception {
    //事件时间
    long currentEventTime = value.getWrapped().getEventTime();

    addToStateValuesSet(windowState, currentEventTime, value.getWrapped());
    //数据进入到flink时间
    long ingestionTime = value.getWrapped().getIngestionTimestamp();
    ctx.output(Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);
    //从stream取出数据，进行处理
    Rule rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.getId());

    if (rule == null) {
      // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
      // updated and used in `DynamicKeyFunction`
      // TODO: you may want to handle this situation differently, e.g. by versioning rules and
      //       handling them by the same version throughout the whole pipeline, or by buffering
      //       events waiting for rules to come through
      return;
    }
    BigDecimal aggregateResult = BigDecimal.ZERO;

    Boolean matched  = false;

    if (rule.getRuleState() == Rule.RuleState.ACTIVE) {
      boolean ruleResult = false;
      if (rule.getRuleType() == Rule.RuleType.PLAIN) {
        //获取窗口计算的起始时间
        Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);
        //？？？？ todo
        long cleanupTime = (currentEventTime / 1000) * 1000;
        //注册ontimer的时间
        ctx.timerService().registerEventTimeTimer(cleanupTime);

        SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
        for (Long stateEventTime : windowState.keys()) {
          if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
            aggregateValuesInState(stateEventTime, aggregator, rule);
          }
        }
        aggregateResult = aggregator.getLocalValue();
        ruleResult = rule.apply(aggregateResult);
      } else if (rule.getRuleType() == Rule.RuleType.SCRIPT) {
        String ruleScript = rule.getRuleScript();
         matched = Rule.checkScript(ruleScript, rule);

      }

      log.trace(
          "Rule {} | {} : {} -> {}", rule.getRuleId(), value.getKey(), aggregateResult, ruleResult,matched);

      if (ruleResult) {
        if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
          evictAllStateElements();
        }
        alertMeter.markEvent();
        if (rule.getRuleType() == Rule.RuleType.SCRIPT){
          out.collect(
              new Alert<>(
                  rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
        }else {
          out.collect(
              new Alert<>(
                  rule.getRuleId(), rule, value.getKey(), value.getWrapped(), matched));
        }
      }
    }
  }

  /**
   * 处理rule的信息
   * @param rule
   * @param ctx
   * @param out
   * @throws Exception
   */
  @Override
  public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out)
      throws Exception {
    log.trace("Processing {}", rule);
    BroadcastState<Integer, Rule> broadcastState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    handleRuleBroadcast(rule, broadcastState);
    updateWidestWindowRule(rule, broadcastState);
    if (rule.getRuleState() == RuleState.CONTROL) {
      handleControlCommand(rule, broadcastState, ctx);
    }
  }

  private void handleControlCommand(
      Rule command, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
    ControlType controlType = command.getControlType();
    switch (controlType) {
      case EXPORT_RULES_CURRENT:
        for (Map.Entry<Integer, Rule> entry : rulesState.entries()) {
          ctx.output(Descriptors.currentRulesSinkTag, entry.getValue());
        }
        break;
      case CLEAR_STATE_ALL:
        ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
        break;
      case DELETE_RULES_ALL:
        Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<Integer, Rule> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
          log.trace("Removed {}", ruleEntry.getValue());
        }
        break;
    }
  }

  private boolean isStateValueInWindow(
      Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
    return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
  }

  private void aggregateValuesInState(
      Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
    Set<Event> inWindow = windowState.get(stateEventTime);
    if (COUNT.equals(rule.getAggregateFieldName())
        || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
      for (Event event : inWindow) {
        aggregator.add(BigDecimal.ONE);
      }
    } else {
      for (Event event : inWindow) {
        BigDecimal aggregatedValue =
            FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), event);
        aggregator.add(aggregatedValue);
      }
    }
  }

  private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
      throws Exception {
    Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
    if (widestWindowRule != null && widestWindowRule.getRuleState() == Rule.RuleState.ACTIVE) {
      if (widestWindowRule.getWindowMillis() < rule.getWindowMillis()) {
        broadcastState.put(WIDEST_RULE_KEY, rule);
      }
    }
  }

  /**
   * 基于事件定义的定时器，将不满足条件的事件，清除MapStateValue
   * @param timestamp
   * @param ctx
   * @param out
   * @throws Exception
   */
  @Override
  public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out)
      throws Exception {

    Rule widestWindowRule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);

    Optional<Long> cleanupEventTimeWindow =
        Optional.ofNullable(widestWindowRule).map(Rule::getWindowMillis);
    //清除不满足条件的event；
    Optional<Long> cleanupEventTimeThreshold =
        cleanupEventTimeWindow.map(window -> timestamp - window);

    cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
  }

  private void evictAgedElementsFromWindow(Long threshold) {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        Long stateEventTime = keys.next();
        if (stateEventTime < threshold) {
          keys.remove();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void evictAllStateElements() {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        keys.next();
        keys.remove();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
