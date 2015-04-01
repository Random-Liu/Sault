package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.HashMap;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.japi.Procedure;
import com.pku.sault.engine.util.Logger;

// Hash router with dynamic changing instance number
class InputRouter extends UntypedActor {
    static class TakeOver implements Serializable {
        private static final long serialVersionUID = 1L;
        final int lowerBound;
        final int upperBound;
        TakeOver(int lowerBound, int upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }
    }

	static class RouterMap implements Serializable {
        private static final long serialVersionUID = 1L;
		private HashMap<KeyWrapper, ActorRef> routerTable;

		RouterMap() {
			routerTable = new HashMap<KeyWrapper, ActorRef> ();
		}

		boolean isTargetAvailable(KeyWrapper keyWrapper) {
			return routerTable.get(keyWrapper)  != null;
		}

		ActorRef route(KeyWrapper keyWrapper) {
			return routerTable.get(keyWrapper);
		}

		void setTarget(KeyWrapper keyWrapper, ActorRef target) {
			routerTable.put(keyWrapper, target);
		}

        void remoteTraget(KeyWrapper keyWrapper) { routerTable.remove(keyWrapper); }
	}

    private Logger logger;
    private ActorRef operator; // Only used when report
    private ActorRef originalPort;
    private BoltWorkerFactory workerFactory;
    private RouterMap originalRouterMap;
	private RouterMap routerMap;
    private RouteTree forwardRouteTree;
    private int lowerBound;
    private int upperBound;
    private ActorRef lastTarget; // Used to send probe
    private ActorRef outputRouter; // Used when lastTarget == null
	
	public static Props props(final BoltWorkerFactory workerFactory, final ActorRef outputRouter) {
		return Props.create(new Creator<InputRouter>() {
			private static final long serialVersionUID = 1L;
			public InputRouter create() throws Exception {
				return new InputRouter(workerFactory, outputRouter);
			}
		});
	}

	InputRouter(BoltWorkerFactory workerFactory, ActorRef outputRouter) {
        this.logger = new Logger(Logger.Role.INPUT_ROUTER);
        this.workerFactory = workerFactory;
        this.outputRouter = outputRouter;
		this.routerMap = new RouterMap();
        getContext().become(INITIALIZE);
	}

    private Procedure<Object> INITIALIZE = new Procedure<Object>() {
        @Override
        public void apply(Object msg) {
            if (msg instanceof BoltSubOperator.InitPort) {
                logger.info("Start Initializing.");
                BoltSubOperator.InitPort initPort = (BoltSubOperator.InitPort) msg;
                lowerBound = initPort.lowerBound;
                upperBound = initPort.upperBound;
                originalPort = initPort.originalPort;
                // Sub route tree, used during bolt migration.
                forwardRouteTree = new RouteTree(lowerBound, upperBound, getSelf());
                operator = getSender();
                // If there is no need to fetch originalRouteMap, just start working
                if (originalPort == null) {
                    operator.tell(getSelf(), getSelf()); // Report the input port to the operator
                    getContext().unbecome();
                } else {
                    logger.info("Start Taking Over From " + originalPort);
                    originalPort.tell(new TakeOver(lowerBound, upperBound), getSelf());
                }
            } else if (msg instanceof RouterMap) { // Original route map sent by original port
                logger.info("Get Router Map From " + originalPort);
                originalRouterMap = (RouterMap)msg;
                operator.tell(getSelf(), getSelf());
                getContext().unbecome();
            } else unhandled(msg); // There should never be tuple messages before the port initialized.
            // There should never be Probe either, because latency monitor is suspending.
        }
    };

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof TupleWrapper) {
			TupleWrapper tupleWrapper = (TupleWrapper)msg;
            if (tupleInBounds(tupleWrapper)) {
                ActorRef target = routerMap.route(tupleWrapper.getKey());
                if (target == null) {
                    if (originalRouterMap != null && originalRouterMap.route(tupleWrapper.getKey()) != null)
                        // There is an orignalBolt
                        target = getContext().actorOf(workerFactory.takeOverWorker(tupleWrapper.getKey(),
                                originalPort));
                    else
                        target = getContext().actorOf(workerFactory.worker(tupleWrapper.getKey()));
                    routerMap.setTarget(tupleWrapper.getKey(), target);
                }
                target.forward(msg, getContext());
                lastTarget = target; // Set last target here
            } else {
                // This is an message should be handled by other sub operator.
                // This may happen during the migration progress.
                ActorRef target = forwardRouteTree.route(tupleWrapper);
                target.forward(msg, getContext());
            }
		} else if (msg instanceof TakeOver) {
            // A new sub operator is taking over half of the keys
            TakeOver takeOver = (TakeOver)msg;
            assert forwardRouteTree.canBeSplit(lowerBound);
            int newLowerBound = forwardRouteTree.split(lowerBound, getSender());
            assert (upperBound == takeOver.upperBound && newLowerBound == takeOver.lowerBound);
            // Just make sure that the new sub operator is taking over the right part
            upperBound = newLowerBound - 1;
            getSender().tell(routerMap, getSelf()); // Send the original route map
            lastTarget = null; // Discard last target because of splitting
        } else if (msg instanceof BoltWorker.TakeOver) { // Forward command messages to the bolt
            // Input Router will forward command message no matter it is in bound or not.
            // We don't send the message to the original actor directly, because
            // we want to make sure that all the messages sent from router to bolt
            // before we get the state are fully processed.
            BoltWorker.TakeOver takeOver = (BoltWorker.TakeOver)msg;
            ActorRef target = routerMap.route(takeOver.key);
            assert target != null; // There must be a target
            target.forward(msg, getContext());
            routerMap.remoteTraget(takeOver.key); // Remove it, because it will shutdown
        } else if (LatencyMonitor.isProbe(msg)) {
            forwardProbe(msg);
        } else unhandled(msg);
	}

    // Used for latency monitor
    private void forwardProbe(Object msg) {
        // The input router will forward probe to the target of last message,
        // so that to make sure that the probe is sent to a worker which is active
        // recently.
        if (lastTarget != null) lastTarget.forward(msg, getContext());
            // Forward probe to the outputRouter directly if there is no available target
        else outputRouter.forward(msg, getContext());
    }

    private boolean tupleInBounds(TupleWrapper tupleWrapper) {
        int tupleHashCode = tupleWrapper.getKey().hashCode();
        return tupleHashCode >= lowerBound && tupleHashCode <= upperBound;
    }
}