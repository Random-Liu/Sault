package com.pku.sault.app.wordcount;

import com.pku.sault.api.*;

class Emitter extends Spout {
	private Collector collector;
	private long now;
    private long timeoutTable[] = new long[46];
    private final long interval = 10000; // 10s
    private long lastTimeout = 0L;

	Emitter () {
		setInstanceNumber(4);
		setParallelism(16);

        timeoutTable[0] = 100000L;
        timeoutTable[1] = 50000L;
        timeoutTable[2] = 20000L;
        timeoutTable[3] = 10000L;
        timeoutTable[4] = 5000L;
        timeoutTable[5] = 1000L;
        timeoutTable[6] = 1000L;
        timeoutTable[7] = 1000L;
        timeoutTable[8] = 1000L;
        timeoutTable[9] = 10000L;
        timeoutTable[10] = 10000L;
        timeoutTable[11] = 20000L;
        timeoutTable[12] = 20000L;
        timeoutTable[13] = 50000L;
        timeoutTable[14] = 50000L;
        timeoutTable[15] = 100000L;
        timeoutTable[16] = 100000L;
        timeoutTable[17] = 500000L;
        timeoutTable[18] = 500000L;
        timeoutTable[19] = 1000000L;
        timeoutTable[20] = 1000000L;
        timeoutTable[21] = 1000000L;
        timeoutTable[22] = 1000000L;
        timeoutTable[23] = 100000L;
        timeoutTable[24] = 50000L;
        timeoutTable[25] = 20000L;
        timeoutTable[26] = 10000L;
        timeoutTable[27] = 5000L;
        timeoutTable[28] = 1000L;
        timeoutTable[29] = 1000L;
        timeoutTable[30] = 1000L;
        timeoutTable[31] = 1000L;
        timeoutTable[32] = 10000L;
        timeoutTable[33] = 10000L;
        timeoutTable[34] = 20000L;
        timeoutTable[35] = 20000L;
        timeoutTable[36] = 50000L;
        timeoutTable[37] = 50000L;
        timeoutTable[38] = 100000L;
        timeoutTable[39] = 100000L;
        timeoutTable[40] = 500000L;
        timeoutTable[41] = 500000L;
        timeoutTable[42] = 1000000L;
        timeoutTable[43] = 1000000L;
        timeoutTable[44] = 1000000L;
        timeoutTable[45] = 1000000L;
    }

	@Override
	public void open(Collector collector) {
		this.collector = collector;
		now = System.currentTimeMillis();
	}

	@Override
	public long nextTuple() {
        for (int i = 0; i < 128; ++i) {
            collector.emit(new Tuple(""
                    + (char) (Math.random() * 26 + 'a')
                    + (char) (Math.random() * 26 + 'a')
                    + (char) (Math.random() * 3 + 'a') // 26 * 26 *3 = 2028 words
                    , new Tuple(System.currentTimeMillis(), 1)));
        }
        long newNow = System.currentTimeMillis();
        int timeoutIndex = (int)((newNow - now) / interval);
        if (timeoutIndex >= timeoutTable.length)
            timeoutIndex = timeoutTable.length - 1;
        long timeout = timeoutTable[timeoutIndex];
        if (lastTimeout != timeout)
            System.out.println("Timeout " + System.currentTimeMillis() / 1000 + " " + timeout);
        lastTimeout = timeout;
        return timeout;
    }

	@Override
	public void close() {
		// Do nothing now
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {

	}
}

class Counter extends Bolt {
	private static final long serialVersionUID = 1L;

	Counter (int parallelism) {
		setInitialParallelism(parallelism);
		setMinParallelism(1);
		setMaxLatency(300);
	}

	private Collector collector;
	private String word;
	private int wordCount;
	private long averageTime = 0L;
	private long now;
	private long sample_interval = 2000;

	@Override
	public void prepare(Collector collector) {
		this.collector = collector;
		this.wordCount = 0;
		this.now = System.currentTimeMillis();
	}

	@Override
	public void execute(Tuple tuple) {
		if (word == null)
			word = (String)tuple.getKey();
		averageTime += System.currentTimeMillis() - (Long)((Tuple) tuple.getValue()).getKey();
		wordCount += (Integer) ((Tuple) tuple.getValue()).getValue();
		long newNow = System.currentTimeMillis();
		if (newNow - now >= sample_interval) {
			now = newNow;
			if (Math.random() <= 0.005 * getInitialParallelism())
                System.out.println("Latency " + System.currentTimeMillis() / 1000 + " " + (double) averageTime / wordCount);
			averageTime = 0L;
			this.collector.emit(new Tuple(word, wordCount));
			wordCount = 0;
		}
	}

	@Override
	public void cleanup() {
		this.collector.emit(new Tuple(word, wordCount));
	}

	@Override
	public Object get() { return wordCount; }

	@Override
	public void set(Object state) { this.wordCount = (Integer)state; }
}

public class WordCounter {
	public static void main(String[] args) {
		Config config = new Config();
		App app = new App(config);
        app.addNode("Counter", new Counter(10));
        app.addNode("Emitter", new Emitter());
        app.addEdge("Emitter", "Counter");
	}
}