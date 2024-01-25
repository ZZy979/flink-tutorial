package com.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Connected streams example: use a control stream to specify words which must be filtered out of the words stream.
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/etl/#example">Flink documentation</a>
 */
public class WordFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE")
                .keyBy(w -> w);

        DataStream<String> streamOfWords = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE")
                .keyBy(w -> w);

        control.connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration parameters) {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String blockWord, Collector<String> out) throws Exception {
            blocked.update(true);
        }

        @Override
        public void flatMap2(String word, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(word);
            }
        }
    }
}
