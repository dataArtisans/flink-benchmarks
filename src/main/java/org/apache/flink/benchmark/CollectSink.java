package org.apache.flink.benchmark;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pnowojski on 7/5/17.
 */
class CollectSink<T> implements SinkFunction<T> {
	public final List<T> result = new ArrayList<>();

	@Override
	public void invoke(T value) throws Exception {
		result.add(value);
	}
}
