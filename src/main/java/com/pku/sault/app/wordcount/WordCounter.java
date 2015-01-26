package com.pku.sault.app.wordcount;

import com.pku.sault.api.App;
import com.pku.sault.api.Collector;
import com.pku.sault.api.Bolt;
import com.pku.sault.api.Config;
import com.pku.sault.api.Tuple;

public class WordCounter extends Bolt {
	private static final long serialVersionUID = 1L;
	
	private Collector collector;
	private String word;
	private int wordCount;
	private final int MAX_WORD_COUNT = 1000;
	
	// TODO Set timeout function?
	@Override
	public void prepare(Collector collector) {
		System.out.println("Have no idea");
		this.collector = collector;
		this.wordCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		if (word == null)
			word = (String)tuple.getKey();
		this.wordCount += (Integer)tuple.getValue();
		if (wordCount >= MAX_WORD_COUNT)
			this.collector.emit(new Tuple(word, wordCount));
	}

	@Override
	public void cleanup() {
		this.collector.emit(new Tuple(word, wordCount));
	}

	public static void main(String[] args) {
		Config config = new Config();
		App app = new App(config);
		app.addNode("A", new WordCounter());
		app.addNode("B", new WordCounter());
		app.addEdge("A", "B");
	}
}
