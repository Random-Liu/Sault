package com.pku.sault.engine.util;

import akka.util.Timeout;
import scala.concurrent.duration.Duration;

/**
 * System constants.
 * Created by taotaotheripper on 2015/1/28.
 */
public class Constants {
    static final public long FUTURE_TIMEOUT = 120L; // 120 seconds
    static final public Timeout futureTimeout = new Timeout(Duration.create(FUTURE_TIMEOUT, "seconds"));

    static final public String TIMER_DISPATCHER = "sault.timer-dispatcher";
}
