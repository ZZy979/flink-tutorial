package com.example.streaming;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Keyed state example: de-duplicate a stream of events, so that you only keep the first event with each key.
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/etl/#an-example-with-keyed-state">Flink documentation: An Example with Keyed State</a>
 */
public class EventDeduplicator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = env.fromElements(
                new Event("a", 1), new Event("b", 2), new Event("a", 2),
                new Event("c", 3), new Event("b", 3), new Event("d", 4)
        );
        events.keyBy(e -> e.key)
                .flatMap(new Deduplicator())
                .print();

        env.execute();
    }

    public static class Event {
        public String key;
        public long timestamp;

        public Event(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return key + '@' + timestamp;
        }
    }

    public static class Deduplicator extends RichFlatMapFunction<Event, Event> {
        private ValueState<Boolean> keyHasBeenSeen;

        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("keyHasBeenSeen", Types.BOOLEAN);
            keyHasBeenSeen = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Event event, Collector<Event> out) throws Exception {
            if (keyHasBeenSeen.value() == null) {
                out.collect(event);
                keyHasBeenSeen.update(true);
            }
        }
    }
}
