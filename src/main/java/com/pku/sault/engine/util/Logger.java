package com.pku.sault.engine.util;

import java.io.PrintStream;
import java.util.Date;

/**
 * Logger used in the system.
 * Created by taotaotheripper on 2015/1/26.
 */
public class Logger {
    private enum Level {
        INFO,
        WARN,
        ERROR,
        DEBUG
    }

    public enum Role {
        WORKER,
        LATENCY_MONITOR,
        INPUT_ROUTER,
        OUTPUT_ROUTER,
        SUB_OPERATOR,
        OPERATOR,
        RESOURCE_MANAGER,
        DRIVER
    }

    private boolean enabled;

    private Role role;

    public Logger(Role role) {
        this.role = role;
        this.enabled = true;
    }

    private void log(Level level, Role role, String msg) {
        PrintStream ps;
        if (level == Level.INFO || level == Level.DEBUG) ps = System.out;
        else ps = System.err;
        ps.println("[" + level + "] [" + new Date() + "] [" + role + "] " + msg);
    }

    public void info(String msg) {
        if (enabled)
            log(Level.INFO, role, msg);
    }

    public void warn(String msg) {
        if (enabled)
            log(Level.WARN, role, msg);
    }

    public void error(String msg) {
        if (enabled)
            log(Level.ERROR, role, msg);
    }

    public void debug(String msg) {
        if (enabled)
            log(Level.DEBUG, role, msg);
    }

    public void disable() {
        enabled = false;
    }
}
