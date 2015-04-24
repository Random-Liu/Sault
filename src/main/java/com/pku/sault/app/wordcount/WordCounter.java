package com.pku.sault.app.wordcount;

import com.pku.sault.api.*;

class Emitter extends Spout {
	private Collector collector;
	//int x = 0;
	private long now;

	Emitter () {
		// TODO setInstanceNumber(16) => dead lock, why?
		setInstanceNumber(4);
		setParallelism(16);
	}

	@Override
	public void open(Collector collector) {
		this.collector = collector;
		now = System.currentTimeMillis();
	}

	@Override
	public long nextTuple() {
		//System.out.println("Emitting");
		for (int i = 0; i < 128; ++i) {
			collector.emit(new Tuple(""
					+ (char) (Math.random() * 26 + 'a')
					+ (char) (Math.random() * 26 + 'a')
					+ (char) (Math.random() * 3 + 'a') // 26 * 26 *3 = 2028 words
					, new Tuple(System.currentTimeMillis(), 1)));
		}
		long newNow = System.currentTimeMillis();
		if (newNow - now < 10000)
			return 50000L;
		//	return 1000000L; //2s
		else if (newNow - now < 20000)
			return 20000L;
		else if (newNow - now < 30000)
			return 10000L;
		else if (newNow - now < 40000)
			return 5000L;
		else
			return 1000L;
		//else	return 1000;
		//else
			//return 1000;
		//++x;
		//return 60000000;
		//return 20000; // Every 1s send test once, just for test
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
		setReactionFactor(20);
		setInitialParallelism(parallelism);
		setMaxLatency(300);
	}

	private Collector collector;
	private String word;
	private int wordCount;
	private final int MAX_WORD_COUNT = 1000;
	private long averageTime = 0L;
	private long now;
	private long sample_interval = 2000;

	@Override
	public void prepare(Collector collector) {
		// Current now don't need config.
		// System.out.println("Have no idea");
		this.collector = collector;
		this.wordCount = 0;
		this.now = System.currentTimeMillis();
	}

	@Override
	public void execute(Tuple tuple) {
		if (word == null)
			word = (String)tuple.getKey();
		// System.out.println(word + " " + wordCount);
		averageTime += System.currentTimeMillis() - (Long)((Tuple) tuple.getValue()).getKey();
		wordCount += (Integer) ((Tuple) tuple.getValue()).getValue();
		//if (wordCount >= MAX_WORD_COUNT) {
		long newNow = System.currentTimeMillis();
		if (newNow - now >= sample_interval) {
			now = newNow;
			if (Math.random() <= 0.001 * getInitialParallelism()) {
				System.out.println(word + " " + (double) averageTime / wordCount + "");
			}
			averageTime = 0L;
			this.collector.emit(new Tuple(word, wordCount));
			// System.out.println(word + " " + wordCount);
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
		System.out.println(app.addNode("Counter", new Counter(2)));
		System.out.println(app.addNode("Emitter", new Emitter()));
		System.out.println(app.addNode("Emitter", new Emitter()));
		// app.deactivate();
		System.out.println(app.addEdge("Emitter", "Counter"));
		System.out.println(app.addEdge("Emit", "Counter"));
		System.out.println(app.addEdge("Emitter", "Count"));
		System.out.println(app.addEdge("Emitter", "Counter"));
		/*try {
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		// app.activate();
		/*
        try {
            Thread.sleep(8000);
        } catch (Exception e) {
            e.printStackTrace();
        }

		System.out.println("Do merging!!!!!!!!!!!!!!!!!!!!!!!");
		System.out.println(app.mergeNode("Emitter"));
		System.out.println(app.mergeNode("Counter"));

        System.out.println("Do splitting!!!!!!!!!!!!!!!!!!!!!!!");
        System.out.println(app.splitNode("Emitter"));
        System.out.println(app.splitNode("Counter"));*/
	}
}