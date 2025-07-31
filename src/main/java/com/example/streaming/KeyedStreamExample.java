package com.example.streaming;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/etl/#keyed-streams">Flink documentation: Keyed Streams</a>
 */
public class KeyedStreamExample {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: KeyedStreamExample {sum|maxBy}");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String type = args[0];
        if (type.equals("sum")) {
            DataStream<Tuple2<String, Integer>> wordCounts = env
                    .fromElements("to be or not to be".split(" "))
                    .map(w -> Tuple2.of(w, 1))
                    .returns(Types.TUPLE(Types.STRING, Types.INT));
            wordCounts.keyBy(t -> t.f0).sum(1).print();
        } else if (type.equals("maxBy")) {
            DataStream<PageView> pageViews = env.fromElements(
                    new PageView(1, 2, 5),
                    new PageView(2, 1, 12),
                    new PageView(1, 3, 8),
                    new PageView(3, 2, 15),
                    new PageView(2, 3, 2));
            DataStream<PageView> longestStay = pageViews.keyBy(pv -> pv.userId).max("duration");
            longestStay.print();
        }

        env.execute();
    }

    public static class PageView {
        public int userId;
        public int pageId;
        public int duration;

        public PageView() {}

        public PageView(int userId, int pageId, int duration) {
            this.userId = userId;
            this.pageId = pageId;
            this.duration = duration;
        }

        @Override
        public String toString() {
            return "PageView{userId=" + userId + ", pageId=" + pageId + ", duration=" + duration + '}';
        }
    }
}
