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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * Benchmark for serializing POJOs and Tuples with different serializers.
 */
public class PojoSerializationBenchmarks extends BenchmarkBase {

	private static final int RECORDS_PER_INVOCATION = 100_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + PojoSerializationBenchmarks.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	@OperationsPerInvocation(value = PojoSerializationBenchmarks.RECORDS_PER_INVOCATION)
	public void testPojoSerializer() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = PojoSerializationBenchmarks.RECORDS_PER_INVOCATION)
	public void testTupleSerializer() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);

		env.addSource(new TupleSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = PojoSerializationBenchmarks.RECORDS_PER_INVOCATION)
	public void testKryoSerializer() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);
		env.getConfig().enableForceKryo();

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = PojoSerializationBenchmarks.RECORDS_PER_INVOCATION)
	public void testKryoSerializerRegistered() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.enableForceKryo();
		executionConfig.registerKryoType(MyPojo.class);
		executionConfig.registerKryoType(MyOperation.class);

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	private abstract static class BaseSource<T> implements ParallelSourceFunction<T> {
		private static final long serialVersionUID = 8318018060123048234L;

		final int numKeys;
		int remainingEvents;

		BaseSource(int numEvents, int numKeys) {
			this.remainingEvents = numEvents;
			this.numKeys = numKeys;
		}

		@Override
		public void cancel() {
			this.remainingEvents = 0;
		}
	}

	private static class PojoSource extends BaseSource<MyPojo> {
		private static final long serialVersionUID = 2941333602938145526L;

		PojoSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		public void run(SourceContext<MyPojo> out) {

			int keyId = 0;

			MyPojo template =
					new MyPojo(
							keyId,
							"myName",
							new String[] {"op1", "op2", "op3", "op4"},
							new MyOperation[] {
									new MyOperation(1, "op1"),
									new MyOperation(2, "op2"),
									new MyOperation(3, "op3")},
							new int[] {1, 2, 3},
							null);


			while (--remainingEvents >= 0) {
				synchronized (out.getCheckpointLock()) {
					template.setId(keyId++);
					out.collect(template);
				}
				if (keyId >= numKeys) {
					keyId = 0;
				}
			}
		}
	}

	private static class TupleSource extends BaseSource<Tuple6<Integer, String, String[], Tuple2<Integer, String>[], int[], Object>> {
		private static final long serialVersionUID = 2941333602938145526L;

		TupleSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		public void run(SourceContext<Tuple6<Integer, String, String[], Tuple2<Integer, String>[], int[], Object>> out) {

			int keyId = 0;

			Tuple6<Integer, String, String[], Tuple2<Integer, String>[], int[], Object> template =
					new MyPojo(
							keyId,
							"myName",
							new String[] {"op1", "op2", "op3", "op4"},
							new MyOperation[] {
									new MyOperation(1, "op1"),
									new MyOperation(2, "op2"),
									new MyOperation(3, "op3")},
							new int[] {1, 2, 3},
							null).toTuple();


			while (--remainingEvents >= 0) {
				synchronized (out.getCheckpointLock()) {
					template.setField(keyId++, 0);
					out.collect(template);
				}
				if (keyId >= numKeys) {
					keyId = 0;
				}
			}
		}
	}

	public static class MyPojo {
		public int id;
		private String name;
		private String[] operationNames;
		private MyOperation[] operations;
		private int[] operationIds;
		private Object nullable;

		public MyPojo() {
		}

		public MyPojo(
				int id,
				String name,
				String[] operationNames,
				MyOperation[] operations,
				int[] operationIds,
				Object nullable) {
			this.id = id;
			this.name = name;
			this.operationNames = operationNames;
			this.operations = operations;
			this.operationIds = operationIds;
			this.nullable = nullable;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String[] getOperationNames() {
			return operationNames;
		}

		public void setOperationNames(String[] operationNames) {
			this.operationNames = operationNames;
		}

		public MyOperation[] getOperations() {
			return operations;
		}

		public void setOperations(
				MyOperation[] operations) {
			this.operations = operations;
		}

		public int[] getOperationIds() {
			return operationIds;
		}

		public void setOperationIds(int[] operationIds) {
			this.operationIds = operationIds;
		}

		public Object getNullable() {
			return nullable;
		}

		public void setNullable(Object nullable) {
			this.nullable = nullable;
		}

		public Tuple6<Integer, String, String[], Tuple2<Integer, String>[], int[], Object> toTuple() {
			Tuple2[] operationTuples = new Tuple2[operations.length];
			for (int i = 0; i < operations.length; ++i) {
				operationTuples[i] = Tuple2.of(operations[i].id, operations[i].name);
			}
			return Tuple6.of(id, name, operationNames, operationTuples, operationIds, nullable);
		}
	}

	public static class MyOperation {
		int id;
		protected String name;

		public MyOperation() {
		}

		public MyOperation(int id, String name) {
			this.id = id;
			this.name = name;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
