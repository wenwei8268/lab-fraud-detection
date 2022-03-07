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

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

import com.ververica.field.dynamicrules.Event;
import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.KeysExtractor;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.Transaction;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** Implements dynamic data partitioning based on a set of broadcasted rules. */
@Slf4j
public class DynamicKeyFunction
    extends BroadcastProcessFunction<Event, Rule, Keyed<Event, String, Integer>> {

  private RuleCounterGauge ruleCounterGauge;

  @Override
  public void open(Configuration parameters) {
    ruleCounterGauge = new RuleCounterGauge();
    getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
  }

  @Override
  public void processElement(
      Event event, ReadOnlyContext ctx, Collector<Keyed<Event, String, Integer>> out)
      throws Exception {
    //通过descriptor，获取state map
    ReadOnlyBroadcastState<Integer, Rule> rulesState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    forkEventForEachGroupingKey(event, rulesState, out);
  }

  private void forkEventForEachGroupingKey(
      Event event,
      ReadOnlyBroadcastState<Integer, Rule> rulesState,
      Collector<Keyed<Event, String, Integer>> out)
      throws Exception {
    int ruleCounter = 0;
    for (Map.Entry<Integer, Rule> entry : rulesState.immutableEntries()) {
      final Rule rule = entry.getValue();
      //在处理transaction的流当中，将transaction流按照rule中的 group by字段进行拆分，将同一个事件按照rule拆分成多份
      out.collect(
          new Keyed<>(
              event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getRuleId()));
      ruleCounter++;
    }
    ruleCounterGauge.setValue(ruleCounter);
  }

  @Override
  public void processBroadcastElement(
      Rule rule, Context ctx, Collector<Keyed<Event, String, Integer>> out) throws Exception {
    log.trace("Processing {}", rule);
    //通过descriptor，获取到规则的map state的值，
    BroadcastState<Integer, Rule> broadcastState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    //如果rule的状态更新了，更新rule的规则
    handleRuleBroadcast(rule, broadcastState);
    //处理control指令，则按照control处理
    if (rule.getRuleState() == RuleState.CONTROL) {
      handleControlCommand(rule.getControlType(), broadcastState);
    }
  }

  private void handleControlCommand(
      ControlType controlType, BroadcastState<Integer, Rule> rulesState) throws Exception {
    if (controlType == ControlType.DELETE_RULES_ALL) {
      Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
      while (entriesIterator.hasNext()) {
        Entry<Integer, Rule> ruleEntry = entriesIterator.next();
        rulesState.remove(ruleEntry.getKey());
        log.trace("Removed {}", ruleEntry.getValue());
      }
    }
  }

  private static class RuleCounterGauge implements Gauge<Integer> {

    private int value = 0;

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public Integer getValue() {
      return value;
    }
  }
}
