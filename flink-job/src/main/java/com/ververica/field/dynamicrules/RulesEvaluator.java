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

package com.ververica.field.dynamicrules;

import static com.ververica.field.config.Parameters.CHECKPOINT_INTERVAL;
import static com.ververica.field.config.Parameters.LOCAL_EXECUTION;
import static com.ververica.field.config.Parameters.LOCAL_MODE_DISABLE_WEB_UI;
import static com.ververica.field.config.Parameters.MIN_PAUSE_BETWEEN_CHECKPOINTS;
import static com.ververica.field.config.Parameters.OUT_OF_ORDERNESS;
import static com.ververica.field.config.Parameters.RULES_SOURCE;
import static com.ververica.field.config.Parameters.SINK_PARALLELISM;
import static com.ververica.field.config.Parameters.SOURCE_PARALLELISM;
import static org.apache.flink.configuration.RestOptions.BIND_PORT;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.functions.AverageAggregate;
import com.ververica.field.dynamicrules.functions.DynamicAlertFunction;
import com.ververica.field.dynamicrules.functions.DynamicKeyFunction;
import com.ververica.field.dynamicrules.sinks.AlertsSink;
import com.ververica.field.dynamicrules.sinks.CurrentRulesSink;
import com.ververica.field.dynamicrules.sinks.LatencySink;
import com.ververica.field.dynamicrules.sources.RulesSource;
import com.ververica.field.dynamicrules.sources.TransactionsSource;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

@Slf4j
public class RulesEvaluator {

  private Config config;

  RulesEvaluator(Config config) {
    this.config = config;
  }

  public void run() throws Exception {

    // Environment setup
    StreamExecutionEnvironment env = configureStreamExecutionEnvironment();

    // Streams setup
    DataStream<Rule> rulesUpdateStream = getRulesUpdateStream(env);
    DataStream<Event> transactions = getTransactionsStream(env);

    BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

    // Processing pipeline setup
    DataStream<Alert> alerts =
        transactions
            //链接数据源,先动态处理 partitionKey
            .connect(rulesStream)
            //key by 之前 用BroadcastProcessFunction
            .process(new DynamicKeyFunction())
            .uid("DynamicKeyFunction")
            .name("Dynamic Partitioning Function")
            .keyBy((keyed) -> keyed.getKey())
            //处理和rule
            .connect(rulesStream)
            // key by 之后 用KeyedBroadcastProcessFunction
            .process(new DynamicAlertFunction())
            .uid("DynamicAlertFunction")
            .name("Dynamic Rule Evaluation Function");
    //测流输出，迟到的数据
    DataStream<Long> latency =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.latencySinkTag);

    DataStream<Rule> currentRules =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.currentRulesSinkTag);

    DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alerts);
    DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);

    int sinkParallelism = config.get(SINK_PARALLELISM);

    alertsJson
        .addSink(AlertsSink.createAlertsSink(config))
        .setParallelism(sinkParallelism)
        .name("Alerts JSON Sink");

    currentRulesJson
        .addSink(CurrentRulesSink.createRulesSink(config))
        .setParallelism(sinkParallelism)
        .name("Rules Export Sink");

    DataStream<String> latencies =
        latency
            .timeWindowAll(Time.seconds(10))
            .aggregate(new AverageAggregate())
            .map(String::valueOf);
    latencies.addSink(LatencySink.createLatencySink(config)).name("Latency Sink");

    env.execute("Fraud Detection Engine");
  }

  private DataStream<Event> getTransactionsStream(StreamExecutionEnvironment env) {
    // Data stream setup
    SourceFunction<String> transactionSource = TransactionsSource.createTransactionsSource(config);
    int sourceParallelism = config.get(SOURCE_PARALLELISM);
    DataStream<String> transactionsStringsStream =
        env.addSource(transactionSource)
            .name("Transactions Source")
            .setParallelism(sourceParallelism);
    DataStream<Event> transactionsStream =
        TransactionsSource.stringsStreamToTransactions(transactionsStringsStream);
    return transactionsStream.assignTimestampsAndWatermarks(
        new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS)));
  }

  private DataStream<Rule> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {

    RulesSource.Type rulesSourceEnumType = getRulesSourceType();

    SourceFunction<String> rulesSource = RulesSource.createRulesSource(config);
    DataStream<String> rulesStrings =
        env.addSource(rulesSource).name(rulesSourceEnumType.getName()).setParallelism(1);
    return RulesSource.stringsStreamToRules(rulesStrings);
  }

  private RulesSource.Type getRulesSourceType() {
    String rulesSource = config.get(RULES_SOURCE);
    return RulesSource.Type.valueOf(rulesSource.toUpperCase());
  }

  private StreamExecutionEnvironment configureStreamExecutionEnvironment() {
    final String localMode = config.get(LOCAL_EXECUTION);

    StreamExecutionEnvironment env;
    if (localMode.isEmpty() || localMode.equals(LOCAL_MODE_DISABLE_WEB_UI)) {
      // cluster mode or disabled web UI
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    } else {
      Configuration flinkConfig = new Configuration();
      flinkConfig.set(BIND_PORT, localMode);
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
    }

    if (!localMode.isEmpty()) {
      // slower restarts inside the IDE and other local runs
      env.setRestartStrategy(
          RestartStrategies.fixedDelayRestart(
              10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
    }

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getCheckpointConfig().setCheckpointInterval(config.get(CHECKPOINT_INTERVAL));
    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS));

    return env;
  }

  private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends Event>
      extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
      super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
      return element.getEventTime();
    }
  }

  public static class Descriptors {
    public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
        new MapStateDescriptor<>(
            "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

    public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {
    };
    public static final OutputTag<Rule> currentRulesSinkTag =
        new OutputTag<Rule>("current-rules-sink") {
        };
  }
}
