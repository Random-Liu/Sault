package com.pku.sault.api;

/**
 * Bolt processing tuples.
 * @author taotaotheripper
 *
 */
public abstract class Bolt extends Task {
	/* In Bolt default PARALLELISM is just an initial concurrency,
	 * in fact when the framework is running it will change dynamically.
	 * However, a good initial concurrency can reduce the concurrency changing time.
	 */
	public int MIN_PARALLELISM = 2;
	public int MAX_PARALLELISM = 16;

	protected Bolt() {
		this.INSTANCE_NUMBER = 0; // For bolt is in dynamic instance mode in default
	}
	public abstract void prepare(Collector collector);
	public abstract void execute(Tuple tuple);
	public abstract void cleanup();
}
