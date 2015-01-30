package com.pku.sault.engine.operator;

import akka.actor.ActorContext;
import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Created by taotaotheripper on 2015/1/31.
 */
public class Operator {
    static class Target implements Serializable {
        private static final long serialVersionUID = 1L;
        final String OperatorID;
        final ActorRef actor; // If target == null, it means remove the target
        Target(String OperatorID, ActorRef actor) {
            this.OperatorID = OperatorID;
            this.actor = actor;
        }
    }

    static class RouterRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        final String OperatorID;
        RouterRequest(String OperatorID) {
            this.OperatorID = OperatorID;
        }
    }

    static class Router implements Serializable {
        private static final long serialVersionUID = 1L;
        final String OperatorID;
        final RouteTree router;
        Router(String OperatorID, RouteTree router) {
            this.OperatorID = OperatorID;
            this.router = router;
        }
    }

    /**
     * Add target to operator.
     * @param targetID
     * @param target
     * @param operator
     * @param context
     */
    public static void addTarget(String targetID, ActorRef target, ActorRef operator, ActorContext context) {
        operator.tell(new Target(targetID, target), context.self());
    }

    /**
     * Remove target from operator.
     * TODO The framework have the ability to remove target, try to make use of it later.
     * TODO When to remove source?
     * @param targetID
     * @param operator
     * @param context
     */
    public static void removeTarget(String targetID, ActorRef operator, ActorContext context) {
        // Pass null to remove the target
        operator.tell(new Target(targetID, null), context.self());
    }
}
