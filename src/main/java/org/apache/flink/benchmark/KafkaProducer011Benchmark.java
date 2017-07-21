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

import org.apache.flink.benchmark.functions.LongSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(Throughput)
@Fork(value = 3, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false"})
@OperationsPerInvocation(value = KafkaProducer011Benchmark.RECORDS_PER_INVOCATION)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class KafkaProducer011Benchmark {

	public static final int RECORDS_PER_INVOCATION = 7_000_000;

	@Benchmark
	public void kafkaProducer(ProducerContext context) throws Exception {
		context.env
				.addSource(new LongSource(RECORDS_PER_INVOCATION))
				.addSink(
						new FlinkKafkaProducer011<>(
								context.topic,
								context.keyedSerializationSchema,
								context.standardProperties,
								new FlinkFixedPartitioner<>(),
								context.semanticValue));

		context.env.execute();
	}

	@State(Thread)
	public static class ProducerContext extends KafkaContextBase {
		@Param({"NONE", "EXACTLY_ONCE", "AT_LEAST_ONCE"})
		public String semantic = "EXACTLY_ONCE";

		public FlinkKafkaProducer011.Semantic semanticValue;

		@Setup
		public void setUp() {
			super.setUp();
			semanticValue = FlinkKafkaProducer011.Semantic.valueOf(this.semantic);
		}

		@TearDown
		public void tearDown() {
			super.tearDown();
		}
	}

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + KafkaProducer011Benchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}
}
