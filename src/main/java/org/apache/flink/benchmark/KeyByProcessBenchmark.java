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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.benchmark.functions.LongSource;
import org.apache.flink.benchmark.functions.MultiplyByTwo;
import org.apache.flink.benchmark.functions.SumReduce;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@OperationsPerInvocation(value = KeyByProcessBenchmark.RECORDS_PER_INVOCATION)
public class KeyByProcessBenchmark extends BenchmarkBase {

	public static final int RECORDS_PER_INVOCATION = 10_000_000;
	public static final int KEYS = 16;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + KeyByProcessBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void benchmarkCount(FlinkEnvironmentContext context) throws Exception {

		StreamExecutionEnvironment env = context.env;
		DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_INVOCATION));

		source
				.map(new MultiplyByTwo())
				.keyBy(new KeySelector<Long, Long>() {
					@Override
					public Long getKey(Long value) throws Exception {
						return value % KEYS;
//						return 0L;
					}
				})
				.reduce(new SumReduce())
				.addSink(new SinkFunction<Long>() {
					@Override
					public void invoke(Long value) throws Exception {
					}
				});

		env.execute();
	}

	@Test
	public void test() throws Exception {
		FlinkEnvironmentContext context = new FlinkEnvironmentContext();
		context.setUp();

		benchmarkCount(context);
	}
}
