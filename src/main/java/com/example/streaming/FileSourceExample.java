package com.example.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * File source example: read files in a given directory.
 * Usage: FileSourceExample --path &lt;path&gt; [--mode {batch|stream}] [--interval &lt;seconds&gt;]
 */
public class FileSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool param = ParameterTool.fromArgs(args);

        Path basePath = new Path(param.getRequired("path"));
        FileSource.FileSourceBuilder<String> builder = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), basePath);
        String mode = param.get("mode", "batch");
        if (mode.equals("stream")) {
            int interval = param.getInt("interval", 30);
            builder = builder.monitorContinuously(Duration.ofSeconds(interval));
        }
        FileSource<String> source = builder.build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "File source")
                .print();
        env.execute();
    }
}
