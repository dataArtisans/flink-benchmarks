package org.apache.flink.benchmark.functions;

import org.apache.flink.api.common.functions.MapFunction;

public class MultiplyByTwo implements MapFunction<Long, Long> {
    @Override
    public Long map(Long value) throws Exception {
        return value * 2;
    }
}
