package org.apache.flink.benchmark.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.benchmark.CollectSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregationOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;

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

    public static void preAggregateWithTumblingWindow(
            DataStreamSource<IntegerLongSource.Record> source,
            TumblingEventTimeWindows windowAssigner) {

        source
                .map(new MultiplyIntLongByTwo())
                .transform(
                        "pre-aggregate",
                        TypeInformation.of(new TypeHint<Tuple3<Integer, TimeWindow, LongAccumulator>>() {
                        }),
                        new PreAggregationOperator<>(
                                new RecordRecordVoidAggregateFunction(),
                                record -> record.key,
                                TypeInformation.of(Integer.class),
                                TypeInformation.of(LongAccumulator.class),
                                windowAssigner,
                                false))
                .map(new MapFunction<Tuple3<Integer, TimeWindow, LongAccumulator>, IntegerLongSource.Record>() {
                    @Override
                    public IntegerLongSource.Record map(Tuple3<Integer, TimeWindow, LongAccumulator> value) throws Exception {
                        return new IntegerLongSource.Record(value.f0, value.f2.value);
                    }
                })
                .keyBy(record -> record.key)
                .window(windowAssigner)
                .reduce(new SumReduceIntLong())
                .addSink(new CollectSink());
    }

    public static class LongAccumulator {
        public long value;

        public LongAccumulator() {
            this(0L);
        }

        public LongAccumulator(long value) {
            this.value = value;
        }
    }

    private static class RecordRecordVoidAggregateFunction
            implements AggregateFunction<IntegerLongSource.Record, LongAccumulator, Void>, Serializable {
        @Override
        public LongAccumulator createAccumulator() {
            return new LongAccumulator();
        }

        @Override
        public void add(IntegerLongSource.Record value, LongAccumulator accumulator) {
            accumulator.value += value.value;
        }

        @Override
        public Void getResult(LongAccumulator accumulator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LongAccumulator merge(LongAccumulator a, LongAccumulator b) {
            return new LongAccumulator(a.value + b.value);
        }
    }
}
