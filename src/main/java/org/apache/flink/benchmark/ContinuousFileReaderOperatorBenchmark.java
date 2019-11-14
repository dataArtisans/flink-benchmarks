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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Preconditions;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.nio.file.Paths;

/**
 * Measures simple file reading and, optionally, parsing operation.
 * Defaults to reading 1000 records from: <ol>
 *     <li>single userdata.avro file</li>
 *     <li>10 text files (100 lines each)</li>
 *     <li>100 text (smaller) files (10 lines each)</li></ol>
 * By default it runs with DOP 1 and then 2.
 * <p>
 * Parameters: <ul>
 *     <li>path - folder or file; if not absolute, it's assumed relative to src/main/resources </li>
 *     <li>fmt - format hint (optional: if not given it is inferred from file extension or starting of folder name)</li>
 *     <li>dop - degree of parallelism.</li>
 * </ul>
 */
// bash script to generate 10 files with 100 random base64 strings each:
// for f in $(seq 1 10); do for l in $(seq 1 100); do dd if=/dev/random count=2 | base64 >> src/main/resources/txt-10-100-1024/$f; done; done
@OperationsPerInvocation(value = ContinuousFileReaderOperatorBenchmark.RECORDS_PER_INVOCATION)
@Fork(value = 1)
public class ContinuousFileReaderOperatorBenchmark extends BenchmarkBase {
    public static final int RECORDS_PER_INVOCATION = 1000;

    private static final long CHECKPOINT_INTERVAL_MS = 100;

    private final String BASE_DIR = "src/main/resources/"; // used if non-absolute path provided

    private File absPath;
    private FileInputFormat<?> fileReader;

    @Param({"avro/userdata.avro", "txt-10-100-1024", "txt-100-10-1024"})
    public String path;

    @Param("")
    public String format;

    @Param({"1", "2"})
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
        fileReader = chooseFormat(absPath.toString(), (format.isEmpty()) ? guessFormat(absPath) : format);

        Preconditions.checkArgument(absPath.exists(), "%s doesn't exist", absPath);
        if (absPath.isDirectory()) Preconditions.checkArgument(absPath.canExecute(), "can't list files in %s", absPath);
        else Preconditions.checkArgument(absPath.canRead(), "can't read %s", absPath);
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

    private static String guessFormat(File path) {
        return path.isFile() ?
                path.toString().replaceAll(".*\\.", "") :
                path.toString().replaceAll(".*/", "").replaceAll("-.*", "");
    }

    private static FileInputFormat<?> chooseFormat(String file, String ext) {
        switch (ext.toLowerCase()) {
            case "avro":
                return new AvroInputFormat<>(new Path(file), Object.class);
            case "txt":
            case "text":
                return new TextInputFormat(new Path(file));
            case "csv":
                return new RowCsvInputFormat(new Path(file), new TypeInformation[]{TypeInformation.of(String.class)});
            default:
                throw new IllegalArgumentException(String.format("can't infer file format for %s using %s", file, ext));
        }
    }

}
