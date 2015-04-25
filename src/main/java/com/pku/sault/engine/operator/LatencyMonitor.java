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
        int rate; // TODO We use message number as rate current now, we'll decide to use block number or message number after we do some experiment
        Probe(ActorRef target, long id) {
            this.target = target;
            this.id = id;
            this.now = System.nanoTime(); // The time unit is ns
            this.rate = 0;
        }
    }

    static class LoadStatus implements Serializable {
        private static final long serialVersionUID = 1L;
        final Map<ActorRef, LOAD_STATUS> loadStatus;
        LoadStatus(Map<ActorRef, LOAD_STATUS> loadStatus) {
            this.loadStatus = loadStatus;
        }
    }

    static enum LOAD_STATUS {
        UNDERLOAD, // Underload time exceed underloadReactionFactor
        LOWLOAD, // Underload time under underloadReactionFactor
        NORMORLLOAD, // No Timeout and no underload
        HIGHLOAD, // Timeout time under overloadReactionFactor
        OVERLOAD  // Timeout time exceed overloadReactionFactor
    }
    private ActorRef operator;
    private final int probePeriod;
    private final double overloadReactionFactor;
    private final int overloadReactionTime;
    private final double underloadReactionFactor;
    private final int underloadReactionTime;
    private final double lowWaterMark;

    private class Probes {
        private class ProbeStatus {
            boolean isProbeTimeout = false;
            boolean isProbeUnderload = false;
            int timeoutTimes = 0;
            int underloadTimes = 0;
            int currentMaxRate = 0;
            Queue<Boolean> timeoutHistory = new LinkedList<Boolean>();
            Queue<Boolean> underloadHistory = new LinkedList<Boolean>();
        }
        private HashMap<ActorRef, ProbeStatus> probesStatus;

        int currentId;
        Probes() {
            probesStatus = new HashMap<ActorRef, ProbeStatus>();
        }

        Probes(List<ActorRef> targets) {
            probesStatus = new HashMap<ActorRef, ProbeStatus>();
            for (ActorRef target : targets)
                probesStatus.put(target, new ProbeStatus());
        }

        void addTarget(ActorRef target) {
            probesStatus.put(target, new ProbeStatus()); // The target will be monitored from next iteration
        }

        void removeTarget(ActorRef target) {
            probesStatus.remove(target);
        }

        void fill(Probe probe) {
            if (probe.id < currentId) return; // This is a timeout probe
            assert probe.id == currentId;
            ProbeStatus probeStatus = probesStatus.get(probe.target);
            probeStatus.isProbeTimeout = false;
            if (probeStatus.currentMaxRate < probe.rate)
                probeStatus.currentMaxRate = probe.rate;
            else if (probe.rate < probeStatus.currentMaxRate * lowWaterMark)
                probeStatus.isProbeUnderload = true;
        }

        // Update load status in targetsLoadStatus, if there is overload or underload,
        // the function will return true which means the latency monitor should report
        // load status to operator.
        boolean updateStatus() {
            boolean needReport = false;
            for (Map.Entry<ActorRef, ProbeStatus> probeStatusEntry : probesStatus.entrySet()) {
                ActorRef target = probeStatusEntry.getKey();
                ProbeStatus probeStatus = probeStatusEntry.getValue();
                // Update timeout status
                if (probeStatus.timeoutHistory.size() >= overloadReactionTime) {
                    boolean isOldestTimeout = probeStatus.timeoutHistory.poll();
                    if (isOldestTimeout) --probeStatus.timeoutTimes;
                }
                probeStatus.timeoutHistory.offer(probeStatus.isProbeTimeout);
                if (probeStatus.isProbeTimeout) {
                    ++probeStatus.timeoutTimes;
                    probeStatus.currentMaxRate = 0; // Invalid max rate
                }

                // Update underload status
                if (probeStatus.underloadHistory.size() >= underloadReactionTime) {
                    boolean isOldestUnderload = probeStatus.underloadHistory.poll();
                    if (isOldestUnderload) --probeStatus.underloadTimes;
                }
                probeStatus.underloadHistory.offer(probeStatus.isProbeUnderload);
                if (probeStatus.isProbeUnderload) ++probeStatus.underloadTimes;

                // Update load status
                if (probeStatus.timeoutTimes >= overloadReactionFactor * overloadReactionTime) {
                    targetsInfo.get(target).loadStatus = LOAD_STATUS.OVERLOAD;
                    needReport = true;
                } else if (probeStatus.timeoutTimes > 0)
                    targetsInfo.get(target).loadStatus = LOAD_STATUS.HIGHLOAD;
                else if (probeStatus.underloadTimes == 0)
                    targetsInfo.get(target).loadStatus = LOAD_STATUS.NORMORLLOAD;
                else if (probeStatus.underloadTimes < underloadReactionFactor * underloadReactionTime)
                    targetsInfo.get(target).loadStatus = LOAD_STATUS.LOWLOAD;
                else {
                    targetsInfo.get(target).loadStatus = LOAD_STATUS.UNDERLOAD;
                    needReport = true;
                }
            }
            ++currentId;
            return needReport;
        }

        Probe newProbe(ActorRef target) {
            ProbeStatus probeStatus = probesStatus.get(target);
            // When create new probe, assume it will timeout, when it is filled we'll set it false
            probeStatus.isProbeTimeout = true;
            probeStatus.isProbeUnderload = false;
            return new Probe(target, currentId);
        }
    }

    private class TargetInfo {
        ActorRef port;
        boolean adaptOver;
        LOAD_STATUS loadStatus;
        TargetInfo(ActorRef port) {
            this.port = port;
            adaptOver = false;
            loadStatus = LOAD_STATUS.NORMORLLOAD;
        }
    }
    private Probes probes;
    private Map<ActorRef, TargetInfo> targetsInfo;

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

    static void setProbeRate(Object msg, int rate) {
        assert (msg instanceof Probe);
        ((Probe) msg).rate = rate;
    }

    LatencyMonitor(List<Pair<ActorRef, ActorRef>> targetPorts, Bolt bolt) {
        this.operator = getContext().parent();
        this.logger = new Logger(Logger.Role.LATENCY_MONITOR);

        probePeriod = bolt.getMaxLatency();
        overloadReactionFactor = bolt.getOverloadReactionFactor();
        overloadReactionTime = bolt.getOverloadReactionTime();
        underloadReactionFactor = bolt.getUnderloadReactionFactor();
        underloadReactionTime = bolt.getUnderloadReactionTime();
        lowWaterMark = bolt.getLowWaterMark();

        startAdaptiveTime = bolt.getStartAdaptiveTime();
        splitAdaptiveTime = bolt.getSplitAdaptiveTime();

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

        probes = new Probes();

        timer = getContext().system().scheduler().schedule(Duration.Zero(),
                Duration.create(probePeriod, TimeUnit.MILLISECONDS), getSelf(), TICK,
                getContext().dispatcher(), getSelf());

        logger.info("Latency Monitor Started");
    }

    private Procedure<Object> REPORTING = new Procedure<Object>() {
        @Override
        public void apply(Object msg) {
            if (msg.equals(DONE)) {
                logger.info("Latency Monitor Start Monitoring");
                getContext().unbecome();
                // Start timer again
                timer = getContext().system().scheduler().schedule(Duration.Zero(),
                        Duration.create(probePeriod, TimeUnit.MILLISECONDS), getSelf(), TICK,
                        getContext().dispatcher(), getSelf());
            } else handleControlMessage(msg); // Ignore all probes and still handle regular messages
        }
    };

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg.equals(TICK)) {
            if (probes.updateStatus()) {
                timer.cancel(); // Stop timer
                report();
                getContext().become(REPORTING);
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
            // logger.info("Current rate: " + ((Probe) msg).rate);
        } else handleControlMessage(msg); // Handle regular messages
    }

    private void report() {
        Map<ActorRef, LOAD_STATUS> targetsLoadStatus = new HashMap<ActorRef, LOAD_STATUS>();
        for (Map.Entry<ActorRef, TargetInfo> targetInfoEntry : targetsInfo.entrySet()) {
            ActorRef target = targetInfoEntry.getKey();
            LOAD_STATUS loadStatus = targetInfoEntry.getValue().loadStatus;
            targetsLoadStatus.put(target, loadStatus);
            if (loadStatus == LOAD_STATUS.UNDERLOAD || loadStatus == LOAD_STATUS.OVERLOAD) {
                probes.removeTarget(target);
                targetsInfo.get(target).adaptOver = false; // Avoid sending probes
                // Remove target, because:
                // 1. Invalid history record, because the targets will be split or merged.
                // 2. Avoid statistic during adapting.
                getContext().system().scheduler().scheduleOnce(Duration.create(splitAdaptiveTime, TimeUnit.SECONDS),
                        getSelf(), new AdaptOver(target),
                        getContext().dispatcher(), getSelf());
            }
        }
        logger.info("Report load status to operator.");
        operator.tell(new LoadStatus(targetsLoadStatus), getSelf());
    }

    private void handleControlMessage(Object msg) {
        if (msg instanceof Target) {
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