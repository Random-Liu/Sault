package com.pku.ebolt.api;

public interface IEBolt {
	public abstract void prepare(Collector collector);
	public abstract void execute(Tuple tuple);
	public abstract void cleanup();
}
