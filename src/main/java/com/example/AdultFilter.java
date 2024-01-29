package com.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This example takes a stream of records about people as input, and filters it to only include the adults.
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/datastream_api/#a-complete-example">Flink documentation: Intro to the DataStream API</a>
 */
public class AdultFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2)
        );

        DataStream<Person> adults = flintstones.filter(person -> person.age >= 18);
        adults.print();

        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return name + ": age " + age.toString();
        }
    }
}
