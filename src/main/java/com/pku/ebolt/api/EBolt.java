package com.pku.ebolt.api;

/*
 * A empty implementation of IEBolt
 */
public class EBolt implements IEBolt {
	private Collector collector;
	
	public void prepare(Collector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		// Just a pass on
		collector.emit(tuple);
	}

	public void cleanup() {
	}
}
