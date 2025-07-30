package com.example.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Joining streams example:
 * <ul>
 *     <li>Window join joins the elements of two streams that share a common key and lie in the same window.</li>
 *     <li>Interval join joins elements of two streams with a common key and where elements of stream B have timestamps that lie in a relative time interval to timestamps of elements in stream A.</li>
 * </ul>
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/joining/">Flink documentation: Joining</a>
 */
public class JoiningStreams {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String type = params.getRequired("type");
        if (type.equals("tumbling")) {
            tumblingWindowJoin();
        } else if (type.equals("sliding")) {
            slidingWindowJoin();
        } else if (type.equals("session")) {
            sessionWindowJoin();
        } else if (type.equals("interval")) {
            intervalJoin();
        } else if (type.equals("cogroup")) {
            coGroupJoin();
        } else {
            System.out.println("Usage: JoiningStreams --type {tumbling,sliding,session,interval,cogroup}");
        }
    }

    public static void tumblingWindowJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> orangeStream = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());
        DataStream<Integer> greenStream = env.fromElements(0, 1, 3, 4)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());

        orangeStream.join(greenStream)
                .where(x -> 0)
                .equalTo(x -> 0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .apply(new MyJoinFunction())
                .print();

        env.execute();
    }

    public static void slidingWindowJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> orangeStream = env.fromElements(0, 1, 2, 3, 4)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());
        DataStream<Integer> greenStream = env.fromElements(0, 3, 4)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());

        orangeStream.join(greenStream)
                .where(x -> 0)
                .equalTo(x -> 0)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(2), Time.milliseconds(1)))
                .apply(new MyJoinFunction())
                .print();

        env.execute();
    }

    public static void sessionWindowJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> orangeStream = env.fromElements(1, 2, 5, 6, 8, 9)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());
        DataStream<Integer> greenStream = env.fromElements(0, 4, 5)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());

        orangeStream.join(greenStream)
                .where(x -> 0)
                .equalTo(x -> 0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(1)))
                .apply(new MyJoinFunction())
                .print();

        env.execute();
    }

    public static void intervalJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> orangeStream = env.fromElements(0, 2, 3, 4, 5, 7)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());
        DataStream<Integer> greenStream = env.fromElements(0, 1, 6, 7)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());

        orangeStream
                .keyBy(x -> 0)
                .intervalJoin(greenStream.keyBy(x -> 0))
                .between(Time.milliseconds(-2), Time.milliseconds(1))
                .process(new MyProcessJoinFunction())
                .print();

        env.execute();
    }

    public static void coGroupJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> orangeStream = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());
        DataStream<Integer> greenStream = env.fromElements(0, 1, 3, 4, 8)
                .assignTimestampsAndWatermarks(getWatermarkStrategy());

        orangeStream.coGroup(greenStream)
                .where(x -> 0)
                .equalTo(x -> 0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .apply(new MyCoGroupFunction())
                .print();

        env.execute();
    }

    private static WatermarkStrategy<Integer> getWatermarkStrategy() {
        return WatermarkStrategy
                .<Integer>noWatermarks()
                .withTimestampAssigner((element, recordTimestamp) -> (long) element);
    }

    private static class MyJoinFunction implements JoinFunction<Integer, Integer, String> {
        @Override
        public String join(Integer first, Integer second) {
            return first + "," + second;
        }
    }

    private static class MyProcessJoinFunction extends ProcessJoinFunction<Integer, Integer, String> {
        @Override
        public void processElement(Integer left, Integer right, Context ctx, Collector<String> out) {
            out.collect(left + "," + right);
        }
    }

    private static class MyCoGroupFunction implements CoGroupFunction<Integer, Integer, String> {
        @Override
        public void coGroup(Iterable<Integer> first, Iterable<Integer> second, Collector<String> out) {
            if (!first.iterator().hasNext()) {
                second.forEach(b -> out.collect("null," + b));
            } else if (!second.iterator().hasNext()) {
                first.forEach(a -> out.collect(a + ",null"));
            } else {
                first.forEach(a -> second.forEach(b -> out.collect(a + "," + b)));
            }
        }
    }

}
