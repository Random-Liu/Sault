package com.pku.sault.api;

import java.io.Serializable;

/**
 * Created by taotaotheripper on 2015/1/27.
 */
public abstract class Spout implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    private int parallelism = 4;
    private int instanceNumber = 4;

    public abstract void open(Collector collector);
    public abstract long nextTuple(); // Return sleep time (us)
    public abstract void close();
    public abstract void activate();
    public abstract void deactivate();

    public int getParallelism() {
        return parallelism;
    }

    protected void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getInstanceNumber() {
        return instanceNumber;
    }

    protected void setInstanceNumber(int instanceNumber) {
        this.instanceNumber = instanceNumber;
    }

    // TODO Implement these functions later
    // public final void activate() {}
    // public final void deactivate() {}

    // Expose clone function
    public Spout clone() throws CloneNotSupportedException {
        return (Spout)super.clone();
    }
}
