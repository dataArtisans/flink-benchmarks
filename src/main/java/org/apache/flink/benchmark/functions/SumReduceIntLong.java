package org.apache.flink.benchmark.functions;

import org.apache.flink.api.common.functions.ReduceFunction;

public class SumReduceIntLong implements ReduceFunction<IntegerLongSource.Record> {
    @Override
    public IntegerLongSource.Record reduce(IntegerLongSource.Record var1, IntegerLongSource.Record var2) throws Exception {
        return IntegerLongSource.Record.of(var1.key, var1.value + var2.value);
    }
}
