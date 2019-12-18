package org.apache.flink.benchmark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
public class FlinkEnvironmentContext {

    public static final int NUM_NETWORK_BUFFERS = 32768; // this value is temporally set to the same as before flip49

    public final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

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

    protected Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, NUM_NETWORK_BUFFERS);
        configuration.setString("taskmanager.network.memory.max", "8m");
        configuration.setString("taskmanager.network.memory.min", "4m");
        return configuration;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return StreamExecutionEnvironment.createLocalEnvironment(1, createConfiguration());
    }
}
