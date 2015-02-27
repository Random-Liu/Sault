package com.pku.sault.engine.operator;

import akka.actor.*;
import akka.japi.Creator;
import akka.japi.Procedure;
import com.pku.sault.api.Bolt;
import com.pku.sault.engine.util.Logger;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by taotaotheripper on 2015/2/24.
 */
class LatencyMonitor extends UntypedActor {
    private static class Probe implements Serializable {
        private static final long serialVersionUID = 1L;
        final long id;
        final long now;
        Probe(long id) {
            this.id = id;
            this.now = System.nanoTime(); // The time unit is ns
        }
    }

    private ActorRef operator;
    private ActorRef inputRouter;
    private ActorRef subOperator;
    private final int probePeriod;
    private final int reactionTime;

    private class Probes {
        private boolean[] probes;
        private int queueSize;
        private long oldestId = 0;
        private long newestId = 0;
        Probes(int size) {
            queueSize = size;
            probes = new boolean[size];
        }

        void fill(Probe probe) {
            if (probe.id < oldestId) return; // This is a timeout probe
            assert probe.id < newestId; // This should be always true
            probes[id(probe.id)] = true;
        }

        boolean timeout() {
            // If the queue is not full, the oldest probe must not be timeout
            if (newestId - oldestId <= queueSize) return false;
            assert newestId - oldestId == queueSize;
            return !probes[id(oldestId++)];
        }

        Probe newProbe() {
            // There must be empty cell in the queue
            assert newestId - oldestId < queueSize;
            probes[id(newestId)] = false;
            return new Probe(newestId++);
        }

        void clear() {
            oldestId = newestId;
        }

        private int id(long longId) {
            return (int)(longId % (long)queueSize);
        }
    }

    private Probes probes;

    static private final String INVOKE = "INVOKE";
    static private final String TICK = "TICK";
    private Cancellable timer;
    private int timeoutTime = 0;

    private Logger logger;

    static Props props(final ActorRef operator, final Bolt bolt) {
        return Props.create(new Creator<LatencyMonitor>() {
            private static final long serialVersionUID = 1L;
            public LatencyMonitor create() throws Exception {
                return new LatencyMonitor(operator, bolt);
            }
        });
    }

    static void invoke(ActorRef latencyMonitor, ActorContext context) {
        latencyMonitor.tell(INVOKE, context.self());
    }

    static boolean isProbe(Object msg) {
        return msg instanceof Probe;
    }

    LatencyMonitor(ActorRef operator, Bolt bolt) {
        this.operator = operator;
        this.subOperator = getContext().parent();
        this.reactionTime = bolt.getReactionTime();
        this.logger = new Logger(Logger.Role.LATENCY_MONITOR);

        int maxLatency = bolt.getMaxLatency();
        int probeFrequency = bolt.getProbeFrequency();
        // 1. Send probe message every probe period
        // 2. Check whether the oldest probe is timeout and remove it.
        // 3. If all the probes during reaction time are timeout, split the sub-operator
        probes = new Probes(probeFrequency);
        probePeriod = maxLatency / probeFrequency;

        getContext().become(SUSPEND); // Suspend at first, start after input router is initialized.
        logger.info("Latency Monitor Started");
    }

    private Procedure<Object> SUSPEND = new Procedure<Object>() {
        @Override
        public void apply(Object msg) {
            if (msg.equals(INVOKE)) { // Start monitoring
                inputRouter = getSender(); // The inputRouter invoke the monitor
                probes.clear(); // Clear probes, all probes before are invalid.
                timeoutTime = 0; // Initialize the timeout time.
                timer = getContext().system().scheduler().schedule(Duration.Zero(),
                        Duration.create(probePeriod, TimeUnit.MILLISECONDS), getSelf(), TICK,
                        getContext().dispatcher(), getSelf());
                getContext().unbecome();
                logger.info("Latency Monitor Invoked");
                // TODO Deal with probes
            } else unhandled(msg); // Ignore all probes
        }
    };

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg.equals(TICK)) {
            if (probes.timeout()) {
                ++timeoutTime;
                if (timeoutTime == reactionTime) {
                    logger.info("Too Many Messages Timeout");
                    operator.tell(new BoltOperator.Split(subOperator), getSelf());
                    logger.info("Ask Operator to Do Split");
                    // Suspend latency monitor, until split and migration is done.
                    // The inputRouter will inform the monitor when it finishes
                    // migration.
                    getContext().become(SUSPEND);
                    logger.info("Latency Monitor Suspend");
                    return; // All the clear thing will be done in SUSPEND
                }
            } else timeoutTime = 0;
            inputRouter.tell(probes.newProbe(), getSelf());
        } else if (msg instanceof Probe) {
            probes.fill((Probe)msg);
            long now = System.nanoTime();
            // TODO Temporary log here, remove or format this later
            logger.info("Current latency: " + (now - ((Probe) msg).now) / 1000 + "us");
        } else unhandled(msg);
    }

    @Override
    public void postStop() {
        timer.cancel();
    }
}
