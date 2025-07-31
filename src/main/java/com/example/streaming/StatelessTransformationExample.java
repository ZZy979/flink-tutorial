package com.example.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/etl/#stateless-transformations">Flink documentation: Stateless Transformations</a>
 */
public class StatelessTransformationExample {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: StatelessTransformationExample {double|wordLength|wordSplit|oddNumber}");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String type = args[0];
        if (type.equals("double")) {
            DataStream<Long> doubled = env.fromSequence(1, 5).map(x -> 2 * x);
            doubled.print();
        } else if (type.equals("wordLength")) {
            DataStream<Integer> wordLengths = env
                    .fromElements("to be or not to be that is the question".split(" "))
                    .map(String::length);
            wordLengths.print();
        } else if (type.equals("wordSplit")) {
            DataStream<String> words = env
                    .fromElements("to be or not to be", "that is the question")
                    .flatMap(new LineSplitter());
            words.print();
        } else if (type.equals("oddNumber")) {
            DataStream<Long> oddNumbers = env.fromSequence(1, 5).flatMap(new OddNumber());
            oddNumbers.print();
        }

        env.execute();
    }

    private static class LineSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String line, Collector<String> out) {
            for (String word : line.split(" ")) {
                out.collect(word);
            }
        }
    }

    private static class OddNumber implements FlatMapFunction<Long, Long> {
        @Override
        public void flatMap(Long x, Collector<Long> out) {
            if (x % 2 == 1) {
                out.collect(x);
            }
        }
    }
}
