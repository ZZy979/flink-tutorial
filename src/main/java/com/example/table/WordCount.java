package com.example.table;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.*;

/**
 * The famous word count example that shows a minimal Flink Table API & SQL job in batch execution mode.<br>
 * Usage: <code>WordCount --mode {stream|batch}</code>
 *
 * @see <a href="https://github.com/apache/flink/blob/release-1.17.2/flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java/basics/WordCountSQLExample.java">Flink official example: WordCountSQLExample.java</a>
 */
public class WordCount {

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        // set up the Table API
        EnvironmentSettings settings;
        if (params.get("mode", "stream").equals("stream"))
            settings = EnvironmentSettings.inStreamingMode();
        else
            settings = EnvironmentSettings.inBatchMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // define table schema
        DataType rowType = DataTypes.ROW(
                DataTypes.FIELD("word", DataTypes.VARCHAR(20)),
                DataTypes.FIELD("frequency", DataTypes.INT())
        );

        // create a table with example data
        Table wordTable = tableEnv.fromValues(
                rowType,
                row("To", 5),
                row("be", 3),
                row("or", 1),
                row("not", 2),
                row("to", 10),
                row("that", 4),
                row("is", 2),
                row("the", 15),
                row("question", 1)
        );
        wordTable.printSchema();

        // query with Table API
        TableResult tableResult = wordTable
                .filter($("word").charLength().isLessOrEqual(5))
                .select($("word").lowerCase().as("word"), $("frequency"))
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("total_frequency"))
                .execute();
        tableResult.print();

        // register a view temporarily
        tableEnv.createTemporaryView("word_table", wordTable);

        // query with SQL
        TableResult sqlResult = tableEnv.sqlQuery(
                "SELECT LOWER(word) AS word, SUM(frequency) AS total_frequency " +
                "FROM word_table " +
                "WHERE CHAR_LENGTH(word) <= 5 " +
                "GROUP BY LOWER(word)"
        ).execute();
        sqlResult.print();
    }
}
