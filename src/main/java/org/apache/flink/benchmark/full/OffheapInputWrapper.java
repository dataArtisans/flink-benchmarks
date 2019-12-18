package org.apache.flink.benchmark.full;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.lang.reflect.Field;

public class OffheapInputWrapper {
    public SpillingAdaptiveSpanningRecordDeserializer<?> reader;
    public Buffer buffer;
    public DataInputView dataInput;

    public OffheapInputWrapper(byte[] initialPayload) throws Exception {
        reader = new SpillingAdaptiveSpanningRecordDeserializer<>(new String[0]);
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledOffHeapMemory(initialPayload.length, this);
        segment.put(0, initialPayload);
        buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, true, initialPayload.length);
        Field nonSpanningWrapper = reader.getClass().getDeclaredField("nonSpanningWrapper");
        nonSpanningWrapper.setAccessible(true);
        dataInput = (DataInputView) nonSpanningWrapper.get(reader);
    }

    public void reset() throws Exception {
        reader.setNextBuffer(buffer);
    }

}
