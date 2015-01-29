package com.pku.sault.app.wordcount;

import com.pku.sault.api.*;

class Emitter extends Spout {
	private Collector collector;

	Emitter () {
		this.INSTANCE_NUMBER = 2;
		this.PARALLELISM = 2;
	}

	@Override
	public void open(Collector collector) {
		this.collector = collector;
	}

	@Override
	public long nextTuple() {
		System.out.println("Emitting");
		collector.emit(new Tuple("test", 1));
		collector.emit(new Tuple("asdf", 1));
		collector.emit(new Tuple("tezxcvst", 1));
		collector.emit(new Tuple("teswrst", 1));
		collector.emit(new Tuple("teqwevst", 1));
		collector.emit(new Tuple("qwer", 1));
		collector.emit(new Tuple("teqwecvst", 1));
		collector.emit(new Tuple("teqwevzxcvxcst", 1));
		collector.emit(new Tuple("asd", 1));
		collector.emit(new Tuple("vxzvb", 1));
		collector.emit(new Tuple("qwerer", 1));
		return 5000; // Every 1s send test once, just for test
	}

	@Override
	public void close() {
		// Do nothing now
	}
}

class Counter extends Bolt {
	private static final long serialVersionUID = 1L;

	private Collector collector;
	private String word;
	private int wordCount;
	private final int MAX_WORD_COUNT = 1000;

	@Override
	public void prepare(Collector collector) {
		// Current now don't need config.
		System.out.println("Have no idea");
		this.collector = collector;
		this.wordCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		if (word == null)
			word = (String)tuple.getKey();
		System.out.println(word + " " + wordCount);
		this.wordCount += (Integer)tuple.getValue();
		if (wordCount >= MAX_WORD_COUNT)
			this.collector.emit(new Tuple(word, wordCount));
	}

	@Override
	public void cleanup() {
		this.collector.emit(new Tuple(word, wordCount));
	}
}

public class WordCounter {
	public static void main(String[] args) {
		Config config = new Config();
		App app = new App(config);
		app.addNode("Counter", new Counter());
		app.addNode("Emitter", new Emitter());
		app.addEdge("Emitter", "Counter");
	}
}
