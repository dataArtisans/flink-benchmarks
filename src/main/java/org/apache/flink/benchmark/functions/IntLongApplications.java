package org.apache.flink.benchmark.functions;

import org.apache.flink.benchmark.CollectSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class IntLongApplications {
    public static <W extends Window> void reduceWithWindow(
            DataStreamSource<IntegerLongSource.Record> source,
            WindowAssigner<Object, W> windowAssigner) {
        source
                .map(new MultiplyIntLongByTwo())
                .keyBy(record -> record.key)
                .window(windowAssigner)
                .reduce(new SumReduceIntLong())
                .addSink(new CollectSink());
    }
}
