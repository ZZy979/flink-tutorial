package com.example.table;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.*;

/**
 * This program explain the concepts of dynamic tables and continuous queries.<br>
 * Usage: <code>ContinuousQueryExample [--window]</code>
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/concepts/dynamic_tables/#dynamic-tables-amp-continuous-queries">Flink documentation: Dynamic Tables & Continuous Queries</a>
 */
public class ContinuousQueryExample {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Click> clickStream = env.fromElements(
                new Click("Mary", "./home", LocalDateTime.of(2021, 7, 21, 12, 0)),
                new Click("Bob", "./cart", LocalDateTime.of(2021, 7, 21, 12, 0)),
                new Click("Mary", "./prod?id=1", LocalDateTime.of(2021, 7, 21, 12, 2)),
                new Click("Mary", "./prod?id=4", LocalDateTime.of(2021, 7, 21, 12, 55)),
                new Click("Bob", "./prod?id=5", LocalDateTime.of(2021, 7, 21, 13, 1)),
                new Click("Liz", "./home", LocalDateTime.of(2021, 7, 21, 13, 30)),
                new Click("Liz", "./prod?id=7", LocalDateTime.of(2021, 7, 21, 13, 59)),
                new Click("Mary", "./cart", LocalDateTime.of(2021, 7, 21, 14, 0)),
                new Click("Liz", "./home", LocalDateTime.of(2021, 7, 21, 14, 2)),
                new Click("Bob", "./prod?id=3", LocalDateTime.of(2021, 7, 21, 14, 30)),
                new Click("Bob", "./home", LocalDateTime.of(2021, 7, 21, 14, 40))
        );

        Schema schema = Schema.newBuilder()
                .column("user", DataTypes.VARCHAR(20))
                .column("url", DataTypes.VARCHAR(100))
                .column("cTime", DataTypes.TIMESTAMP(3))
                .watermark("cTime", $("cTime").minus(lit(5).seconds()))
                .build();
        Table clickTable = tableEnv.fromDataStream(clickStream, schema);
        clickTable.printSchema();

        tableEnv.createTemporaryView("clicks", clickTable);

        if (!params.has("window")) {
            // Example 1: counts the number of visited URLs of each user
            Table userVisitCount = tableEnv.sqlQuery(
                    "SELECT " +
                    "  `user`," +
                    "  COUNT(url) AS cnt " +
                    "FROM clicks " +
                    "GROUP BY `user`"
            );
            System.out.println("Number of visited URLs of each user:");
            tableEnv.toChangelogStream(userVisitCount).print().setParallelism(1);
        }
        else {
            // Example 2: counts the number of visited URLs of each user over an hourly tumbling window
            Table userHourlyVisitCount = tableEnv.sqlQuery(
                    "SELECT " +
                    "  `user`," +
                    "  TUMBLE_END(cTime, INTERVAL '1' HOURS) AS endT," +
                    "  COUNT(url) AS cnt " +
                    "FROM clicks " +
                    "GROUP BY " +
                    "  `user`," +
                    "  TUMBLE(cTime, INTERVAL '1' HOURS)"
            );
            System.out.println("Number of visited URLs of each user over an hourly tumbling window:");
            tableEnv.toDataStream(userHourlyVisitCount).print().setParallelism(1);
        }

        env.execute();
    }

    public static class Click {
        public String user;
        public String url;
        public LocalDateTime cTime;

        public Click() {}

        public Click(String user, String url, LocalDateTime cTime) {
            this.user = user;
            this.url = url;
            this.cTime = cTime;
        }
    }
}
