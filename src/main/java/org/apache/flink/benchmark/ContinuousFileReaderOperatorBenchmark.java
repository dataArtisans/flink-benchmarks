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
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Preconditions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.nio.file.Paths;

/**
 * Measures simple file reading and, optionally, parsing operation.
 * Defaults to reading 100000 records from: <ol>
 *     <li>100 text files (1000 lines each)</li>
 *     <li>1000 text (smaller) files (100 lines each)</li></ol>
 * <p>
 * Parameters: <ul>
 *     <li>path - folder or file; if not absolute, it's assumed relative to src/main/resources </li>
 *     <li>parallelism - degree of parallelism.</li>
 * </ul>
 */
// bash script to generate 100 files with 1000 random base64 lines each:
// for f in $(seq 1 100); do for l in $(seq 1 1000); do dd if=/dev/random count=1 | base64 >> src/main/resources/txt-100-1000-10/$f; done; done
// for f in $(seq 1 1000); do for l in $(seq 1 100); do dd if=/dev/random count=1 | base64 >> src/main/resources/txt-1000-100-10/$f; done; done
@OperationsPerInvocation(value = ContinuousFileReaderOperatorBenchmark.RECORDS_PER_INVOCATION)
public class ContinuousFileReaderOperatorBenchmark extends BenchmarkBase {
    public static final int RECORDS_PER_INVOCATION = 1000;

    private static final long CHECKPOINT_INTERVAL_MS = 100;

    private final String BASE_DIR = "src/main/resources/"; // used if non-absolute path provided

    private File absPath;
    private FileInputFormat<?> fileReader;

    @Param({"txt-100-1000-10", "txt-1000-100-10"})
    public String path;

    @Param("1")
    public String parallelism;

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + ContinuousFileReaderOperatorBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Setup
    public void setUp() {
        absPath = buildPath();
        Preconditions.checkArgument(absPath.exists(), "%s doesn't exist", absPath);
        if (absPath.isDirectory()) Preconditions.checkArgument(absPath.canExecute(), "can't list files in %s", absPath);
        else Preconditions.checkArgument(absPath.canRead(), "can't read %s", absPath);
        fileReader = new TextInputFormat(new Path(absPath.toString()));
    }

    @Benchmark
    public void readFiles(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;

        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS)
                .setParallelism(Integer.parseInt(parallelism))
                .readFile(fileReader, absPath.toString())
                .addSink(new DiscardingSink<>());

        env.execute();
    }

    private File buildPath() {
        java.nio.file.Path p = this.path.startsWith("/") ? Paths.get(this.path) : Paths.get(BASE_DIR, this.path);
        return p.toAbsolutePath().toFile();
    }

}
