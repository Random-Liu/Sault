package com.pku.sault.engine.operator;

import akka.actor.*;
import akka.japi.Creator;
import akka.japi.Pair;
import akka.japi.Procedure;
import com.pku.sault.api.Bolt;
import com.pku.sault.engine.util.Constants;
import com.pku.sault.engine.util.Logger;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by taotaotheripper on 2015/2/24.
 */

class LatencyMonitor extends UntypedActor {
    // TODO Add moving average
    private static class Target implements Serializable {
        private static final long serialVersionUID = 1L;
        final ActorRef target;
        final ActorRef port;
        final boolean toAdd;
        Target(ActorRef target, ActorRef port, boolean toAdd) {
            this.target = target;
            this.port = port;
            this.toAdd = toAdd;
        }
    }

    private static class AdaptOver implements Serializable {
        private static final long serialVersionUID = 1L;
        final ActorRef target;
        AdaptOver(ActorRef target) {
            this.target = target;
        }
    }

    private static class Probe implements Serializable {
        private static final long serialVersionUID = 1L;
        final ActorRef target;
        final long id;
        final long now;
        Probe(ActorRef target, long id) {
            this.target = target;
            this.id = id;
            this.now = System.nanoTime(); // The time unit is ns
        }
    }

    private ActorRef operator;
    private final int probePeriod;
    private final int reactionFactor;

    private class Probes {
        private HashMap<ActorRef, Boolean> probes;
        private HashMap<ActorRef, Integer> timeoutTimes;
        int currentId;
        Probes() {
            probes = new HashMap<ActorRef, Boolean>();
            timeoutTimes = new HashMap<ActorRef, Integer>();
        }

        Probes(List<ActorRef> targets) {
            probes = new HashMap<ActorRef, Boolean>();
            for (ActorRef target : targets)
                probes.put(target, true);
            timeoutTimes = new HashMap<ActorRef, Integer>();
            for (ActorRef target : targets)
                timeoutTimes.put(target, 0);
        }

        void addTarget(ActorRef target) {
            probes.put(target, true); // The target will be monitored from next iteration
            timeoutTimes.put(target, 0);
        }

        void removeTarget(ActorRef target) {
            timeoutTimes.remove(target);
            probes.remove(target);
        }

        void fill(Probe probe) {
            if (probe.id < currentId) return; // This is a timeout probe
            assert probe.id == currentId;
            probes.put(probe.target, true);
            timeoutTimes.put(probe.target, 0);
        }

        // This function will add elements in timeoutTargets
        boolean timeout() {
            timeoutTargets.clear();
            for (Map.Entry<ActorRef, Boolean> probeEntry : probes.entrySet()) {
                if (!probeEntry.getValue()) {
                    int timeoutTime = timeoutTimes.get(probeEntry.getKey());
                    ++timeoutTime;
                    System.out.println("Timeout!!!!!!!!!!!!!!!!!!TimeoutTime: " + timeoutTime);
                    if (timeoutTime >= reactionFactor) {
                        timeoutTargets.offer(probeEntry.getKey());
                        timeoutTimes.put(probeEntry.getKey(), 0);
                        // It's time to do split, we don't want the history
                    } else
                        timeoutTimes.put(probeEntry.getKey(), timeoutTime);
                }
                probeEntry.setValue(false);
            }
            ++currentId; // Probes with old id will be treated as timeout probe
            return !timeoutTargets.isEmpty();
        }

        Probe newProbe(ActorRef target) {
            return new Probe(target, currentId);
        }
    }

    private class TargetInfo {
        ActorRef port;
        boolean adaptOver;
        TargetInfo(ActorRef port) {
            this.port = port;
            adaptOver = false;
        }
    }
    private Probes probes;
    private Map<ActorRef, TargetInfo> targetsInfo;
    private Queue<ActorRef> timeoutTargets = new LinkedList<ActorRef>();

    static private final int TICK = 0;
    static private final int DONE = 1;

    private Cancellable timer;
    private final long startAdaptiveTime;
    private final long splitAdaptiveTime;

    private Logger logger;

    static Props props(final List<Pair<ActorRef, ActorRef>> targetPorts, final Bolt bolt) {
        return Props.create(new Creator<LatencyMonitor>() {
            private static final long serialVersionUID = 1L;
            public LatencyMonitor create() throws Exception {
                return new LatencyMonitor(targetPorts, bolt);
            }
        });
    }

    static void addTarget(ActorRef latencyMonitor, ActorRef target, ActorRef port, ActorContext context) {
        latencyMonitor.tell(new Target(target, port, true), context.self());
    }

    static void removeTarget(ActorRef latencyMonitor, ActorRef target, ActorContext context) {
        latencyMonitor.tell(new Target(target, null, false), context.self());
    }

    static void done(ActorRef latencyMonitor, ActorContext context) {
        latencyMonitor.tell(DONE, context.self());
    }

    static boolean isProbe(Object msg) {
        return msg instanceof Probe;
    }

    LatencyMonitor(List<Pair<ActorRef, ActorRef>> targetPorts, Bolt bolt) {
        this.operator = getContext().parent();
        this.logger = new Logger(Logger.Role.LATENCY_MONITOR);

        probePeriod = bolt.getMaxLatency();
        reactionFactor = bolt.getReactionFactor();
        startAdaptiveTime = bolt.getStartAdaptiveTime();
        splitAdaptiveTime = bolt.getSplitAdaptiveTime();

        // List<ActorRef> targets = new LinkedList<ActorRef>();
        targetsInfo = new HashMap<ActorRef, TargetInfo>();
        for (Pair<ActorRef, ActorRef> targetPort : targetPorts) {
            ActorRef target = targetPort.first();
            ActorRef port = targetPort.second();
            logger.debug("Add target " + target);
            targetsInfo.put(target, new TargetInfo(port));
            // targets.add(targetPort.first());
            getContext().system().scheduler().scheduleOnce(Duration.create(startAdaptiveTime, TimeUnit.SECONDS),
                    getSelf(), new AdaptOver(target),
                    getContext().dispatcher(), getSelf());
        }

        //probes = new Probes(targets);
        probes = new Probes();

        timer = getContext().system().scheduler().schedule(Duration.Zero(),
                Duration.create(probePeriod, TimeUnit.MILLISECONDS), getSelf(), TICK,
                getContext().dispatcher(), getSelf());

        logger.info("Latency Monitor Started");
    }

    private Procedure<Object> SPLITTING = new Procedure<Object>() {
        @Override
        public void apply(Object msg) {
            if (msg.equals(DONE)) { // Start monitoring
                if (!timeoutTargets.isEmpty()) {
                    ActorRef timeoutTarget = timeoutTargets.poll();
                    logger.info("Too Many Messages Timeout");
                    operator.tell(new BoltOperator.Split(timeoutTarget), getSelf());
                    logger.info("Ask Operator to Do Split");
                    targetsInfo.get(timeoutTarget).adaptOver = false;
                    getContext().system().scheduler().scheduleOnce(Duration.create(splitAdaptiveTime, TimeUnit.SECONDS),
                            getSelf(), new AdaptOver(timeoutTarget),
                            getContext().dispatcher(), getSelf());
                } else {
                    getContext().unbecome();
                    logger.info("Latency Monitor Start Monitoring");
                    // Start timer again
                    // TODO Just test here
                    timer = getContext().system().scheduler().schedule(Duration.Zero(),
                            Duration.create(probePeriod, TimeUnit.MILLISECONDS), getSelf(), TICK,
                            getContext().dispatcher(), getSelf());
                }
            } else unhandled(msg); // Ignore all probes
        }
    };

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg.equals(TICK)) {
            if (probes.timeout()) {
                timer.cancel(); // Stop timer
                getContext().become(SPLITTING);
                getSelf().tell(DONE, getSelf()); // Start splitting
                logger.info("Latency Monitor Start Splitting");
                return;
            }
            // Send probes to all targets
            for (Map.Entry<ActorRef, TargetInfo> targetInfoEntry : targetsInfo.entrySet()) {
                ActorRef target = targetInfoEntry.getKey();
                TargetInfo targetInfo = targetInfoEntry.getValue();
                if (targetInfo.adaptOver) // Only probe target which has done adapting
                    targetInfo.port.tell(probes.newProbe(target), getSelf());
            }
        } else if (msg instanceof Probe) {
            probes.fill((Probe)msg);
            long now = System.nanoTime();
            // TODO Temporary log here, remove or format this later
            // logger.info("SubOperator: " + ((Probe) msg).target + " Current latency: " + (now - ((Probe) msg).now) / 1000 + "us");
        } else if (msg instanceof Target) {
            Target target = (Target)msg;
            if (target.toAdd) {
                logger.debug("Add target " + target.target);
                targetsInfo.put(target.target, new TargetInfo(target.port));
                getContext().system().scheduler().scheduleOnce(Duration.create(startAdaptiveTime, TimeUnit.SECONDS),
                        getSelf(), new AdaptOver(target.target),
                        getContext().dispatcher(), getSelf());
            } else {
                logger.debug("Remove target " + target.target);
                probes.removeTarget(target.target);
                targetsInfo.remove(target.target);
            }
        } else if (msg instanceof AdaptOver) { // Adapt over
            AdaptOver adaptOver = (AdaptOver)msg;
            logger.debug("Adapt over " + adaptOver.target);
            if (targetsInfo.containsKey(adaptOver.target)) { // It may be removed before
                probes.addTarget(adaptOver.target);
                targetsInfo.get(adaptOver.target).adaptOver = true;
            }
        } else unhandled(msg);
    }

    @Override
    public void postStop() {
        timer.cancel();
    }
}