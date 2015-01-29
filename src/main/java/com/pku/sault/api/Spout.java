package com.pku.sault.api;

import java.io.Serializable;

/**
 * Created by taotaotheripper on 2015/1/27.
 */
public abstract class Spout extends Task {
    public abstract void open(Collector collector);
    public abstract long nextTuple(); // Return sleep time (ms)
    public abstract void close();

    // TODO Implement these functions later
    // public final void activate() {}
    // public final void deactivate() {}
}
