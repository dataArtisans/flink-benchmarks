package org.apache.flink.benchmark.functions;

import org.apache.flink.api.common.functions.ReduceFunction;

public class ValidatingCounter<T> implements ReduceFunction<T> {
    private long expectedCount;
    private long count = 0;

    public ValidatingCounter(long expectedCount) {
        this.expectedCount = expectedCount;
    }

    @Override
    public T reduce(T value1, T value2) throws Exception {
        count++;
        if (count >= expectedCount) {
            throw new SuccessException();
        }
        return value1;
    }
}
