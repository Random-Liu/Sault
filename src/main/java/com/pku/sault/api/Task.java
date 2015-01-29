package com.pku.sault.api;

import java.io.Serializable;

/**
 * Created by taotaotheripper on 2015/1/27.
 */
public class Task implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    /**
     * TODO Make a decision later
     * Design decisions:
     * 1. Fix instance  + dynamic parallelism: Shuffle: ok; Grouping: no. Spout: no
     * 2. Fix instance + fix parallelism: Shuffle: ok; Grouping: ok; Spout: ok
     * 3. Dynamic instance + fix parallelism: Shuffle: ok(In fact also fix); Grouping: ok; Spout: no
     * 4. Dynamic instance + dynamic parallelism: Shuffle: ok(In fact also fix); Grouping: ok; Spout: no
     * Shuffle: Fix instance (with Akka router) + dynamic/fix parallelism
     * Grouping: (Fix instance + fix parallelism) / (Dynamic instance + dynamic/fix parallelism)
     * Spout: Fix instance + fix parallelism
     */
    /**
     * INSTANCE_NUMBER: Instance number on each parallel executor. If instance number <= 0,
     * it means the instance dynamic mode, in which mode the instance number is changing
     * dynamically with the input.
     */
    public int INSTANCE_NUMBER = 4; // Default instance number is 4
    public int PARALLELISM = 4; // Default parallelism is 4
    // TODO Add parallelism control later

    public Task clone() {
        try {
            return (Task)super.clone();
        } catch(CloneNotSupportedException e) {
            e.printStackTrace();
            return null;
        }
    }
}
