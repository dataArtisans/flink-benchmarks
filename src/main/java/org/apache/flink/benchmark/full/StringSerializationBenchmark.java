package org.apache.flink.benchmark.full;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.core.memory.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StringSerializationBenchmark extends BenchmarkBase {

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + StringSerializationBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Param({"ascii", "russian", "chinese"})
    public String type;

    @Param({"4", "128", "16384"})
    public String lengthStr;

    int length;
    String input;
    public static final char[] asciiChars = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890".toCharArray();
    public static final char[] russianChars = "йцукенгшщзхъфывапролджэячсмитьбюЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ".toCharArray();
    public static final char[] chineseChars = "的是不了人我在有他这为之大来以个中上们到国说和地也子要时道出而于就下得可你年生".toCharArray();

    ExecutionConfig config = new ExecutionConfig();
    TypeSerializer<String> serializer = TypeInformation.of(String.class).createSerializer(config);
    DataOutputSerializer serializedStream;
    OffheapInputWrapper offheapInput;

    public static final int INVOCATIONS = 1000;

    @Setup
    public void setup() throws Exception {
        length = Integer.parseInt(lengthStr);
        switch (type) {
            case "ascii":
                input = generate(asciiChars, length);
                break;
            case "russian":
                input = generate(russianChars, length);
                break;
            case "chinese":
                input = generate(chineseChars, length);
                break;
            default:
                throw new IllegalArgumentException(type + "charset is not supported");
        }
        serializedStream = new DataOutputSerializer(128);
        DataOutputSerializer payloadWriter = new DataOutputSerializer(128);
        for (int i = 0; i < INVOCATIONS; i++) {
            serializer.serialize(input, payloadWriter);
        }
        byte[] payload = payloadWriter.getCopyOfBuffer();
        offheapInput = new OffheapInputWrapper(payload);
    }

    @Benchmark
    @OperationsPerInvocation(INVOCATIONS)
    public int stringWrite() throws IOException {
        serializedStream.pruneBuffer();
        for (int i = 0; i < INVOCATIONS; i++) {
            serializer.serialize(input, serializedStream);
        }
        return serializedStream.length();
    }

    @Benchmark
    @OperationsPerInvocation(INVOCATIONS)
    public void stringRead(Blackhole bh) throws Exception {
        offheapInput.reset();
        for (int i = 0; i < INVOCATIONS; i++) {
            bh.consume(serializer.deserialize(offheapInput.dataInput));
        }
    }

    private String generate(char[] charset, int length) {
        char[] buffer = new char[length];
        Random random = new Random();
        for (int i=0; i<length; i++) {
            buffer[i] = charset[random.nextInt(charset.length)];
        }
        return new String(buffer);
    }

}
