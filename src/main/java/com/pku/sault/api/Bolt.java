package com.pku.sault.api;

import java.io.Serializable;

/**
 * Bolt processing tuples.
 * @author taotaotheripper
 * TODO Add Stateless Bolt later
 */
public abstract class Bolt implements Cloneable, Serializable {
	private static final long serialVersionUID = 1L;

	private int minParallelism = 2;
	private int maxParallelism = 16;
	private int initialParallelism = 4;
	private int maxInstanceNumber = 1024; // TODO More consideration later

    // [Caution] prepare and cleanup will also be called during migration
	public abstract void prepare(Collector collector);
	public abstract void execute(Tuple tuple);
	public abstract void cleanup();
    public abstract Object get();
    public abstract void set(Object state);

    // Parallelism should be set before added to the graph
	public int getMinParallelism() {
		return minParallelism;
	}

	protected void setMinParallelism(int minParallelism) {
		this.minParallelism = minParallelism;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	protected void setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	public int getInitialParallelism() {
		return initialParallelism;
	}

	protected void setInitialParallelism(int initialParallelism) {
		this.initialParallelism = initialParallelism;
	}

	public int getMaxInstanceNumber() {
		return maxInstanceNumber;
	}

	protected void setMaxInstanceNumber(int maxInstanceNumber) {
		this.maxInstanceNumber = maxInstanceNumber;
	}

	// Expose clone function
	public Bolt clone() throws CloneNotSupportedException {
		return (Bolt)super.clone();
	}
}
