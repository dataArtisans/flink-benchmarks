package org.apache.flink.benchmark.functions;

import org.apache.flink.api.common.functions.ReduceFunction;

public class SumReduce implements ReduceFunction<Long> {
    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
        return value1 + value2;
    }
}
