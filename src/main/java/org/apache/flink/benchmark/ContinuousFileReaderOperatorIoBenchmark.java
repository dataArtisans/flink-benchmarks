/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Preconditions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.io.File;
import java.nio.file.Paths;

@OperationsPerInvocation(value = ContinuousFileReaderOperatorIoBenchmark.RECORDS_PER_INVOCATION)
public class ContinuousFileReaderOperatorIoBenchmark extends BenchmarkBase {

    public static final int RECORDS_PER_INVOCATION = 1000_000;
    private static final long CHECKPOINT_INTERVAL_MS = 100;

    private FileInputFormat<?> fileReader;
    private File path;

    /**
     * Name of the folder where the text files are located. Can be an arbitrary string but the total number of characters
     * should match {@link #RECORDS_PER_INVOCATION}. Here, the following pattern is used: txt-nr_files-nr_lines-nr_symbols.
     */
    @Param({"txt-100-1000-10", "txt-1000-100-10"})
    public String folder;

    @Setup
    public void setUp() {
        java.nio.file.Path p = folder.startsWith("/") ? Paths.get(folder) : Paths.get("src/main/resources/", folder);
        path = p.toAbsolutePath().toFile();
        Preconditions.checkArgument(path.exists() && path.isDirectory() && path.canExecute(), "%s doesn't exist, is not a directory, or isn't readable", path);
        fileReader = new TextInputFormat(new Path(path.toString()));
    }

    @Benchmark
    public void readFiles(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;

        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS)
            .setParallelism(1)
            .readFile(fileReader, path.toString())
            .addSink(new DiscardingSink<>());

        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        env.execute();
    }

}
