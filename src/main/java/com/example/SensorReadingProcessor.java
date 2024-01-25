package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Window function example: finds the peak value from each sensor in 1-minute event time windows,
 * and producing a stream of Tuples containing (key, end-of-window-timestamp, max_value).
 *
 * <p>Records in each window can be processed as a batch, incrementally or with a combination of the two.</p>
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/streaming_analytics/#window-functions"></a>
 */
public class SensorReadingProcessor {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String mode = params.get("mode", "reducing");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> input = env.fromElements(
                new SensorReading(0, "a", 10),
                new SensorReading(10000, "a", 7),
                new SensorReading(20000, "b", 5),
                new SensorReading(40000, "b", 2),
                new SensorReading(50000, "a", 12),
                new SensorReading(60000, "a", 9),
                new SensorReading(80000, "a", 15),
                new SensorReading(90000, "b", 8),
                new SensorReading(100000, "a", 11),
                new SensorReading(100000, "b", 6),
                new SensorReading(110000, "a", 13)
        );

        WatermarkStrategy<SensorReading> strategy = WatermarkStrategy
                .<SensorReading>noWatermarks()
                .withTimestampAssigner((event, timestamp) -> event.timestamp);

        WindowedStream<SensorReading, String, TimeWindow> windowedStream = input
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(x -> x.key)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)));

        DataStream<Tuple3<String, Long, Integer>> result = mode.equals("reducing") ?
                windowedStream.reduce(new MyReducingMax(), new MyWindowFunction()) :
                windowedStream.process(new MyWastefulMax());
        result.print();

        env.execute();
    }

    public static class SensorReading {
        long timestamp;
        String key;
        int value;

        public SensorReading(long timestamp, String key, int value) {
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }
    }

    public static class MyWastefulMax extends ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow> {
        @Override
        public void process(
                String key,
                Context context,
                Iterable<SensorReading> events,
                Collector<Tuple3<String, Long, Integer>> out) {
            int max = 0;
            for (SensorReading event : events) {
                max = Math.max(event.value, max);
            }
            out.collect(Tuple3.of(key, context.window().getEnd(), max));
        }
    }

    public static class MyReducingMax implements ReduceFunction<SensorReading> {
        @Override
        public SensorReading reduce(SensorReading r1, SensorReading r2) {
            return r1.value > r2.value ? r1 : r2;
        }
    }

    public static class MyWindowFunction extends ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow> {
        @Override
        public void process(
                String key,
                Context context,
                Iterable<SensorReading> maxReading,
                Collector<Tuple3<String, Long, Integer>> out) {
            SensorReading max = maxReading.iterator().next();
            out.collect(Tuple3.of(key, context.window().getEnd(), max.value));
        }
    }
}
