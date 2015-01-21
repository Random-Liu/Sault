package com.pku.ebolt.app;

import com.pku.ebolt.api.Collector;
import com.pku.ebolt.api.EBolt;
import com.pku.ebolt.api.Tuple;

public class WordCounter extends EBolt {
	private Collector collector;
	private int wordCount;
	private String word;
	private final int MAX_WORD_COUNT = 1000;
	
	// TODO Set timeout function?
	
	@Override
	public void prepare(Collector collector) {
		this.collector = collector;
		this.wordCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		if (word == null)
			word = (String)tuple.getKey();
		++this.wordCount;
		if (wordCount == MAX_WORD_COUNT)
			this.collector.emit(new Tuple(word, wordCount));
	}

	@Override
	public void cleanup() {
		// Do nothing
	}
}
