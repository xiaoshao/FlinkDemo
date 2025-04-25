package com.zwshao.source.demo.csv.file;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.time.Duration;

public class FileCsvOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TextLineInputFormat textLineInputFormat = new TextLineInputFormat();
        Path path = new Path("./input_data/csv_file/employee.csv");
        FileSource<String> source = FileSource.forRecordStreamFormat(textLineInputFormat, path)
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        DataStreamSource<String> fileSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "fileSource");

        fileSource.print();

        env.execute("start job");
    }
}
