package org.apache.flink.benchmark.operators;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MultiplyByTwoCoStreamMap
		extends AbstractStreamOperator<Long>
		implements TwoInputStreamOperator<Long, Long, Long> {

	@Override
	public void processElement1(StreamRecord<Long> element) {
		output.collect(element.replace(element.getValue() * 2));
	}

	@Override
	public void processElement2(StreamRecord<Long> element) {
		output.collect(element.replace(element.getValue() * 2));
	}
}
