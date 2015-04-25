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

    // Latency monitor configuration
    private int maxLatency = 300; // 300 ms by default
	private double overloadReactionFactor = 0.8; // Overload when 80% of overloadReactionTime is timeout by default
	private int overloadReactionTime = 20; // Reaction in 20 probe period by default, 20 * 300 = 6000ms = 6s
	private double underloadReactionFactor = 0.8; // Underload when 80% of underloadReactionTime is underload by default
	private int underloadReactionTime = 100; // Reaction in 100 probe period by default, 100 & 300 = 30000ms = 30s
	private double lowWaterMark = 0.1; // Underload when rate is under 10% of max rate by default

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

	/* Parallelism API */
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

	/* Elasticity API */
    public int getMaxLatency() {
        return maxLatency;
    }

    protected void setMaxLatency(int maxLatency) {
        this.maxLatency = maxLatency;
    }

	public double getOverloadReactionFactor() {
		return overloadReactionFactor;
	}

	protected void setOverloadReactionFactor(double overloadReactionFactor) {
		assert (overloadReactionFactor <= 1 && overloadReactionFactor >= 0);
		this.overloadReactionFactor = overloadReactionFactor;
	}

	public int getOverloadReactionTime() {
		return overloadReactionTime;
	}

	protected void setOverloadReactionTime(int overloadReactionTime) {
		this.overloadReactionTime = overloadReactionTime;
	}

	public double getUnderloadReactionFactor() {
		return underloadReactionFactor;
	}

	protected void setUnderloadReactionFactor(double underloadReactionFactor) {
		assert (underloadReactionFactor <= 1 && underloadReactionFactor >= 0);
		this.underloadReactionFactor = underloadReactionFactor;
	}

	public int getUnderloadReactionTime() {
		return underloadReactionTime;
	}

	protected void setUnderloadReactionTime(int underloadReactionTime) {
		this.underloadReactionTime = underloadReactionTime;
	}

	public double getLowWaterMark() {
		return lowWaterMark;
	}

	protected void setLowWaterMark(double lowWaterMark) {
		assert (lowWaterMark <=1 && lowWaterMark >= 0);
		this.lowWaterMark = lowWaterMark;
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

	public int getExpiredTimeout() {
		return expiredTimeout;
	}

	protected void setExpiredTimeout(int expiredTimeout) {
		this.expiredTimeout = expiredTimeout;
	}

	// Expose clone function
	public Bolt clone() throws CloneNotSupportedException {
		return (Bolt)super.clone();
	}
}
