package com.pku.sault.api;

import java.io.Serializable;

/**
 * Bolt processing tuples.
 * @author taotaotheripper
 */
public abstract class Bolt implements Cloneable, Serializable {
	public static final int INFINITY_TIMEOUT = -1;
	private static final long serialVersionUID = 1L;
	// * If state is more important:
	// ** Set minParallelism == maxParallelism == initialParallelism, so that the Bolt won't scaling automatically
	// ** Set expiredTimeout == Bolt.INFINITY_TIMEOUT, so that the Bolt will run forever
	//
	// * If dynamic scaling is more important
	// ** Properly configure parallelism, so that the Bolt will elastically scaling in and out.
	// ** Set expiredTimeout == proper timeout, so that the timeout Bolt will be stopped, this is especially useful
	// when elastically scaling.

    // Parallelism configuration
	private int minParallelism = 2;
	private int maxParallelism = 16;
	private int initialParallelism = 4;

    // Latency configuration
    private int maxLatency = 500; // 200 ms by default
    private int reactionFactor = 10; // reaction after 6 timeout probes by default

	// Timeout configuration
	private int expiredTimeout = 30; // 30s by default

	// Time to adaptive new target
	private int startAdaptiveTime = 5; // 5s
	private int splitAdaptiveTime = 2; // 2s

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

    public int getMaxLatency() {
        return maxLatency;
    }

    protected void setMaxLatency(int maxLatency) {
        this.maxLatency = maxLatency;
    }

    public int getReactionFactor() {
        return reactionFactor;
    }

    protected void setReactionFactor(int reactionFactor) {
        this.reactionFactor = reactionFactor;
    }

	public int getExpiredTimeout() {
		return expiredTimeout;
	}

	protected void setExpiredTimeout(int expiredTimeout) {
		this.expiredTimeout = expiredTimeout;
	}

	public int getStartAdaptiveTime() {
		return startAdaptiveTime;
	}

	protected void setStartAdaptiveTime(int startAdaptiveTime) {
		this.startAdaptiveTime = startAdaptiveTime;
	}

	public int getSplitAdaptiveTime() {
		return splitAdaptiveTime;
	}

	protected void setSplitAdaptiveTime(int splitAdaptiveTime) {
		this.splitAdaptiveTime = splitAdaptiveTime;
	}

	// Expose clone function
	public Bolt clone() throws CloneNotSupportedException {
		return (Bolt)super.clone();
	}
}
