package org.apache.flink.benchmark;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
public class FlinkEnvironmentContext {
    public final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    protected final int parallelism = 1;
    protected final boolean objectReuse = true;

    @Setup
    public void setUp() throws IOException {
        // set up the execution environment
        env.setParallelism(parallelism);
        env.getConfig().disableSysoutLogging();
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }

        env.setStateBackend(new MemoryStateBackend());
    }

    public void execute() throws Exception {
        env.execute();
    }
}
