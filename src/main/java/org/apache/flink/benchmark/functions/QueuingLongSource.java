package org.apache.flink.benchmark.functions;

public class QueuingLongSource extends LongSource {

	private static Object lock = new Object();

	private static int currentRank = 1;

	private final int rank;

	public QueuingLongSource(int rank, long maxValue) {
		super(maxValue);
		this.rank = rank;
	}

	@Override
	public void run(SourceContext<Long> ctx) throws Exception {
		synchronized (lock) {
			while (currentRank != rank) {
				lock.wait();
			}
		}

		super.run(ctx);

		synchronized (lock) {
			currentRank++;
			lock.notifyAll();
		}
	}

	public static void reset() {
		currentRank = 1;
	}
}
