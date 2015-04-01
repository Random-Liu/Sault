package com.pku.sault.app.wordcount;

import com.pku.sault.api.*;

class Emitter extends Spout {
	private Collector collector;

	Emitter () {
        // TODO setInstanceNumber(16) => dead lock, why?
		setInstanceNumber(16);
		setParallelism(2);
	}

	@Override
	public void open(Collector collector) {
		this.collector = collector;
	}

	@Override
	public long nextTuple() {
		//System.out.println("Emitting");
		collector.emit(new Tuple("a", new Tuple(System.nanoTime(), 1)));
		collector.emit(new Tuple("b", 1));
		collector.emit(new Tuple("c", 1));
		collector.emit(new Tuple("d", 1));
		collector.emit(new Tuple("e", 1));
		collector.emit(new Tuple("f", 1));
		collector.emit(new Tuple("g", 1));
		collector.emit(new Tuple("h", 1));
		collector.emit(new Tuple("i", 1));
		collector.emit(new Tuple("j", 1));
		collector.emit(new Tuple("k", 1));
        collector.emit(new Tuple("l", 1));
        collector.emit(new Tuple("m", 1));
        collector.emit(new Tuple("n", 1));
        collector.emit(new Tuple("o", 1));
        collector.emit(new Tuple("p", 1));
        collector.emit(new Tuple("q", 1));
        collector.emit(new Tuple("r", 1));
        collector.emit(new Tuple("s", 1));
        collector.emit(new Tuple("t", 1));
        collector.emit(new Tuple("w", 1));
        collector.emit(new Tuple("v", 1));
        collector.emit(new Tuple("u", 1));
        collector.emit(new Tuple("x", 1));
        collector.emit(new Tuple("y", 1));
        collector.emit(new Tuple("z", 1));
		return 1; // Every 1s send test once, just for test
	}

	@Override
	public void close() {
		// Do nothing now
	}
}

class Counter extends Bolt {
	private static final long serialVersionUID = 1L;

    Counter (int parallelism) {
        setInitialParallelism(parallelism);
    }

	private Collector collector;
	private String word;
	private int wordCount;
	private final int MAX_WORD_COUNT = 1000;

	@Override
	public void prepare(Collector collector) {
		// Current now don't need config.
		// System.out.println("Have no idea");
		this.collector = collector;
		this.wordCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		if (word == null)
			word = (String)tuple.getKey();
		// System.out.println(word + " " + wordCount);
		Long time = null;
		if (tuple.getValue() instanceof Tuple) {
			time = (Long)((Tuple) tuple.getValue()).getKey();
			this.wordCount += (Integer) ((Tuple) tuple.getValue()).getValue();
		} else {
			this.wordCount += (Integer) tuple.getValue();
		}
		if (wordCount >= MAX_WORD_COUNT) {
			if (time != null)
				System.out.println("=================== Latency: " + (System.nanoTime() - time)/1000 + " us");
			this.collector.emit(new Tuple(word, wordCount));
			System.out.println(word + " " + wordCount);
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
		System.out.println(app.addEdge("Emitter", "Counter"));
		System.out.println(app.addEdge("Emit", "Counter"));
		System.out.println(app.addEdge("Emitter", "Count"));
		System.out.println(app.addEdge("Emitter", "Counter"));
        /*try {
            Thread.sleep(15000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Do splitting!!!!!!!!!!!!!!!!!!!!!!!");
        System.out.println(app.splitNode("Emitter"));
        System.out.println(app.splitNode("Counter"));*/
	}
}