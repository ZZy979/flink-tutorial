package com.example.table;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * This program generates some order data, performs query using Table API or SQL,
 * and sinks result to CSV file.<br>
 * Usage: <code>UserOrderData --apt-type {table|sql} --output filename</code>
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/common/#query-a-table">Flink documentation: Query a Table</a>
 */
public class UserOrderData {
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Create a source table using table descriptor
        Schema sourceSchema = Schema.newBuilder()
                .column("order_id", DataTypes.VARCHAR(20).notNull())
                .column("user_id", DataTypes.INT())
                .column("product_id", DataTypes.INT())
                .column("price", DataTypes.DECIMAL(10, 2))
                .column("quantity", DataTypes.INT())
                .column("create_time", DataTypes.TIMESTAMP(3))
                .primaryKey("order_id")
                .build();
        TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                .schema(sourceSchema)
                .option(DataGenConnectorOptions.NUMBER_OF_ROWS, 1000L)
                .option("fields.user_id.min", "1")
                .option("fields.user_id.max", "20")
                .option("fields.product_id.min", "1")
                .option("fields.product_id.max", "100")
                .option("fields.price.min", "0.01")
                .option("fields.price.max", "9999.99")
                .option("fields.quantity.min", "1")
                .option("fields.quantity.max", "10")
                .build();
        tableEnv.createTemporaryTable("Orders", sourceDescriptor);
        tableEnv.from("Orders").printSchema();

        // Create a sink table using SQL DDL
        String outFile = params.getRequired("output");
        tableEnv.executeSql(
                "CREATE TABLE User_stats (" +
                "  user_id INT," +
                "  order_count BIGINT," +
                "  total_amount DECIMAL(10, 2)" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + outFile + "'," +
                "  'format' = 'csv'" +
                ")"
        );

        // query a table
        if (params.get("api-type", "table").equals("table")) {
            Table userStats = tableEnv.from("Orders")
                    .groupBy($("user_id"))
                    .select($("user_id"),
                            $("order_id").count().as("order_count"),
                            $("price").times($("quantity")).sum().as("total_amount"))
                    .orderBy($("total_amount").desc());
            userStats.printExplain();
            userStats.insertInto("User_stats").execute();
        }
        else {
            tableEnv.executeSql(
                    "INSERT INTO User_stats " +
                    "SELECT " +
                    "  user_id," +
                    "  COUNT(*) AS order_count," +
                    "  SUM(price * quantity) AS total_amount " +
                    "FROM Orders " +
                    "GROUP BY user_id " +
                    "ORDER BY total_amount DESC"
            );
        }
    }
}
