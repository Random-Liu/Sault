package com.pku.ebolt.app.wordcount;

import com.pku.ebolt.api.Collector;
import com.pku.ebolt.api.EBolt;
import com.pku.ebolt.api.Tuple;

public class WordCounter extends EBolt {
	private Collector collector;
	private String word;
	private int wordCount;
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
		this.wordCount += (Integer)tuple.getValue();
		if (wordCount >= MAX_WORD_COUNT)
			this.collector.emit(new Tuple(word, wordCount));
	}

	@Override
	public void cleanup() {
		this.collector.emit(new Tuple(word, wordCount));
	}
}
