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
        ERROR
    }

    public enum Role {
        WORKER,
        INPUT_ROUTER,
        OUTPUT_ROUTER,
        SUB_OPERATOR,
        OPERATOR,
        RESOURCE_MANAGER,
        DRIVER
    }

    private Role role;

   public  Logger(Role role) {
        this.role = role;
    }

    private void log(Level level, Role role, String msg) {
        PrintStream ps;
        if (level == Level.INFO) ps = System.out;
        else ps = System.err;
        ps.println("[" + level + "] [" + new Date() + "] [" + role + "] " + msg);
    }

    public void info(String msg) {
        log(Level.INFO, role, msg);
    }

    public void warn(String msg) {
        log(Level.WARN, role, msg);
    }

    public void error(String msg) {
        log(Level.ERROR, role, msg);
    }
}
