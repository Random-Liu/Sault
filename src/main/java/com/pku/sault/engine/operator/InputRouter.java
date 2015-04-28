package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.pku.sault.api.Bolt;
import com.pku.sault.api.Tuple;
import com.pku.sault.engine.util.Constants;
import com.pku.sault.engine.util.Logger;
import scala.concurrent.duration.Duration;

// Hash router with dynamic changing instance number
class InputRouter extends UntypedActor {

	static class RouterMap implements Serializable {
        private static final long serialVersionUID = 1L;
        private class TargetInfo {
            ActorRef target;
            Boolean expired;
            TargetInfo(ActorRef target) {
                this.target = target;
                this.expired = false;
            }
        }
		private HashMap<KeyWrapper, TargetInfo> routerTable;

		RouterMap() {
			routerTable = new HashMap<KeyWrapper, TargetInfo> ();
		}

		boolean isTargetAvailable(KeyWrapper keyWrapper) {
			return routerTable.get(keyWrapper)  != null;
		}

		ActorRef route(KeyWrapper keyWrapper) {
            TargetInfo targetInfo = routerTable.get(keyWrapper);
            if (targetInfo == null) return null;
            targetInfo.expired = false;
			return targetInfo.target;
		}

		void setTarget(KeyWrapper keyWrapper, ActorRef target) {
			routerTable.put(keyWrapper, new TargetInfo(target));
		}

        void removeTarget(KeyWrapper keyWrapper) { routerTable.remove(keyWrapper); }

        List<ActorRef> getExpiredTargets() {
            List<ActorRef> expiredTargets = new LinkedList<ActorRef>();

            Iterator<Map.Entry<KeyWrapper, TargetInfo>> targetInfoIterator = routerTable.entrySet().iterator();
            while (targetInfoIterator.hasNext()) {
                Map.Entry<KeyWrapper, TargetInfo> targetInfo = targetInfoIterator.next();
                if (targetInfo.getValue().expired) {
                    expiredTargets.add(targetInfo.getValue().target);
                    targetInfoIterator.remove();
                } else
                    targetInfo.getValue().expired = true;
            }
            for (Map.Entry<KeyWrapper, TargetInfo> targetInfo : routerTable.entrySet()) {

            }
            return expiredTargets;
        }
	}

    private Logger logger;
    private Bolt bolt;
    private final int EXPIRED_TIMEOUT;
    private ActorRef operator; // Only used when report
	private RouterMap routerMap;
    private ActorRef lastTarget; // Used to send probe
    private int receivedMessageSinceLastProbe = 0;
    private ActorRef outputRouter; // Used when lastTarget == null

    private final int TIMEOUT_TICK = 0;
    private Cancellable timer;

    // Just for test
    private boolean testing = false;
    private Cancellable reportTimer;
    private final long reportInterval = 2;
    private final int REPORT = 1;
    private int receivedMessageNumber = 0;
    private int receivedBlockNumber = 0;

	public static Props props(final Bolt bolt, final ActorRef outputRouter) {
		return Props.create(new Creator<InputRouter>() {
			private static final long serialVersionUID = 1L;
			public InputRouter create() throws Exception {
				return new InputRouter(bolt, outputRouter);
			}
		});
	}

	InputRouter(Bolt bolt, ActorRef outputRouter) {
        this.logger = new Logger(Logger.Role.INPUT_ROUTER);
        this.bolt = bolt;
        this.EXPIRED_TIMEOUT = bolt.getExpiredTimeout();
        this.outputRouter = outputRouter;
		this.routerMap = new RouterMap();
        if (EXPIRED_TIMEOUT != Bolt.INFINITY_TIMEOUT) {
            timer = getContext().system().scheduler().schedule(Duration.Zero(),
                    Duration.create(EXPIRED_TIMEOUT, TimeUnit.SECONDS), getSelf(), TIMEOUT_TICK,
                    getContext().system().dispatchers().lookup(Constants.TIMER_DISPATCHER), getSelf());
        }

        if (testing) {
            // Just for test
            this.reportTimer = getContext().system().scheduler().schedule(Duration.Zero(),
                    Duration.create(reportInterval, TimeUnit.SECONDS), getSelf(), REPORT,
                    getContext().system().dispatchers().lookup(Constants.TIMER_DISPATCHER), getSelf());
        }
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof TupleWrapper) {
            routeMessage((TupleWrapper)msg);
            ++receivedBlockNumber;
        } else if (msg instanceof TupleWrapperBlock) {
            TupleWrapperBlock block = (TupleWrapperBlock)msg;
            for (TupleWrapper tupleWrapper : block.getTupleWrappers())
                routeMessage(tupleWrapper);
            ++receivedBlockNumber;
        } else if (LatencyMonitor.isProbe(msg)) {
            forwardProbe(msg);
        } else if (msg.equals(TIMEOUT_TICK)) { // Stop expired targets
            List<ActorRef> expiredTargets = routerMap.getExpiredTargets();
            for (ActorRef expiredTarget : expiredTargets) {
                getContext().stop(expiredTarget);
            }
        } else if (msg.equals(REPORT)) {
            logger.info("Receive " + receivedMessageNumber + " messages in " + receivedBlockNumber + " blocks in 2s");
            receivedMessageNumber = 0;
            receivedBlockNumber = 0;
        } else unhandled(msg);
	}

    private void routeMessage(TupleWrapper tupleWrapper) {
        ++receivedMessageSinceLastProbe;
        // track(tupleWrapper); // Just for test
        ActorRef target = routerMap.route(tupleWrapper.getKey());
        if (target == null) {
            target = getContext().actorOf(BoltWorker.props(tupleWrapper.getKey(), bolt, outputRouter));
            routerMap.setTarget(tupleWrapper.getKey(), target);
        }
        target.forward(tupleWrapper, getContext());
        lastTarget = target; // Set last target here
        ++receivedMessageNumber;
    }

    // Used for latency monitor
    private void forwardProbe(Object msg) {
        // The input router will forward probe to the target of last message,
        // so that to make sure that the probe is sent to a worker which is active
        // recently.
        LatencyMonitor.setProbeRate(msg, receivedMessageSinceLastProbe);
        receivedMessageSinceLastProbe = 0;
        if (lastTarget != null) lastTarget.forward(msg, getContext());
            // Forward probe to the outputRouter directly if there is no available target
        else outputRouter.forward(msg, getContext());
    }

    @Override
    public void postStop() {
        timer.cancel();
        if (reportTimer != null && !reportTimer.isCancelled()) reportTimer.cancel();
    }

    // Just for test
    long averageTime = 0;
    long lastTimeStamp = System.currentTimeMillis();
    long count = 0;
    private void track(TupleWrapper tuple) {
        long now = System.currentTimeMillis();
        averageTime += now - (Long)((Tuple)(tuple.getTuple().getValue())).getKey();
        ++count;
        if (count >= 10000) {
            logger.info("Output to Input Latency: " + (double) averageTime / count + " ms "
                    + (now - lastTimeStamp) + " ms");
            lastTimeStamp = now;
            averageTime = 0L;
            count = 0;
        }
    }
}