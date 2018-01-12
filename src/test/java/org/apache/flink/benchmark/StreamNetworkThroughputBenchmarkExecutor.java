/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.streaming.runtime.io.benchmark.StreamNetworkThroughputBenchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static org.apache.flink.util.Preconditions.checkState;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * JMH throughput benchmark runner.
 */
@OperationsPerInvocation(value = StreamNetworkThroughputBenchmarkExecutor.RECORDS_PER_INVOCATION)
public class StreamNetworkThroughputBenchmarkExecutor extends BenchmarkBase {

	static final int RECORDS_PER_INVOCATION = 5_000_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + StreamNetworkThroughputBenchmarkExecutor.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void networkThroughput(MultiEnvironment context) throws Exception {
		context.executeBenchmark(RECORDS_PER_INVOCATION);
	}

	/**
	 * Setup for the benchmark(s).
	 */
	@State(Thread)
	public static class MultiEnvironment extends StreamNetworkThroughputBenchmark {

		//Ideally we would like to run 1,100ms, 1000,1ms, 1000,100ms. However 1000,1ms is too slow to execute.
		@Param({"1,100ms", "100,1ms", "1000,100ms"})
		public String channelsFlushTimeout = "100,100ms";

		//Do not spam continuous benchmarking with number of writers parameter.
		//@Param({"1", "4"})
		public int writers = 4;

		@Setup
		public void setUp() throws Exception {
			int channels = parseChannels(channelsFlushTimeout);
			int flushTimeout = parseFlushTimeout(channelsFlushTimeout);
			super.setUp(writers, channels, flushTimeout);
		}

		private static int parseFlushTimeout(String channelsFlushTimeout) {
			String[] parameters = channelsFlushTimeout.split(",");
			checkState(parameters.length >= 2);
			String flushTimeout = parameters[1];

			checkState(flushTimeout.endsWith("ms"));
			return Integer.parseInt(flushTimeout.substring(0, flushTimeout.length() - 2));
		}

		private static int parseChannels(String channelsFlushTimeout) {
			String[] parameters = channelsFlushTimeout.split(",");
			checkState(parameters.length >= 1);
			return Integer.parseInt(parameters[0]);
		}

		@TearDown
		public void tearDown() throws Exception {
			super.tearDown();
		}
	}
}
