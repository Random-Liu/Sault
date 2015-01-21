package com.pku.ebolt.api;

/**
 * An empty implementation of IEBolt
 * @author taotaotheripper
 *
 */
public abstract class EBolt {
	/* Default RECOMMEND_CONCURRENCY is 4, this is just
	 * a recommend initial concurrency, in fact when
	 * the framework is running it will change dynamically.
	 * However, a good initial concurrency can reduce
	 * the concurrency changing time.
	 */
	public int INITIAL_CONCURRENCY = 4;
	public int MIN_CONCURRENCY = 2;
	public int MAX_CONCURRENCY = 256;
	public abstract void prepare(Collector collector);
	public abstract void execute(Tuple tuple);
	public abstract void cleanup();
}
