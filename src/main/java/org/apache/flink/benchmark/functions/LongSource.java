package org.apache.flink.benchmark.functions;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class LongSource extends RichParallelSourceFunction<Long> {

    private volatile boolean running = true;
    private long maxValue;

    public LongSource(long maxValue) {
        this.maxValue = maxValue;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long counter = 0;

        while (running) {
            ctx.collectBatch(counter);
            counter++;
            if (counter >= maxValue) {
                cancel();
            }
        }
        ctx.finishBatch();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
