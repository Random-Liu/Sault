package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.dsl.Creators;
import akka.japi.Creator;
import akka.routing.BroadcastPool;
import com.pku.sault.api.Bolt;
import com.pku.sault.api.Spout;
import com.pku.sault.engine.util.Logger;
import scala.reflect.internal.Trees;

class SpoutSubOperator extends UntypedActor {
	private ActorRef manager;
	private ActorRef workerPool;
	private ActorRef outputRouter;
	private Logger logger;

	SpoutSubOperator(Spout spout, Map<String, RouteTree> routerTable) {
		this.outputRouter = getContext().actorOf(OutputRouter.props(routerTable));
		// In fact there is no input, using BroadcastPool so that we can send command
		// to the workers in the future.
		this.workerPool = getContext().actorOf(new BroadcastPool(spout.getInstanceNumber()).props(
				SpoutWorker.props(spout, outputRouter)/*.withDispatcher("sault-dispatcher")*/));
		this.manager = getContext().parent();
		this.logger = new Logger(Logger.Role.SUB_OPERATOR);

		logger.info("Spout Sub-Operator Started.");
	}

	static Props props(final Spout spout, final Map<String, RouteTree> routerTable) {
		return Props.create(new Creator<SpoutSubOperator>() {
			private static final long serialVersionUID = 1L;
			public SpoutSubOperator create() throws Exception {
				return new SpoutSubOperator(spout, routerTable);
			}
		});
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Operator.Router) {
			outputRouter.forward(msg, getContext());
			logger.info("Router updated.");
		} unhandled(msg);
	}
}

class BoltSubOperator extends UntypedActor {
    static class InitPort implements Serializable {
        private static final long serialVersionUID = 1L;
        final int lowerBound;
        final int upperBound;
        final ActorRef originalPort;
        // If originalPort == null, it means that this is a fresh sub operator, it needs not to
        // fetch state from the original port.
        InitPort(int lowerBound, int upperBound, ActorRef originalPort) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.originalPort = originalPort;
        }

        InitPort(int lowerBound, int upperBound) {
            this(lowerBound, upperBound, null);
        }
    }

    private Bolt bolt;
    private ActorRef manager;
	private ActorRef inputRouter;
    private ActorRef latencyMonitor;
	private ActorRef outputRouter;
	private Logger logger;

	BoltSubOperator(Bolt bolt, Map<String, RouteTree> routerTable) {
        this.bolt = bolt;
        this.manager = getContext().parent(); // This hasn't been used.
		// We have moved latencyMonitor to Operator
        // this.latencyMonitor = getContext().actorOf(LatencyMonitor.props(manager, bolt));
        this.outputRouter = getContext().actorOf(OutputRouter.props(routerTable));
        this.inputRouter = getContext().actorOf(InputRouter.props(new BoltWorkerFactory(bolt,
                outputRouter), outputRouter));
		this.logger = new Logger(Logger.Role.SUB_OPERATOR);

		logger.info("Bolt Sub-Operator Started.");
	}

	static Props props(final Bolt bolt, final Map<String, RouteTree> routerTable) {
		return Props.create(new Creator<BoltSubOperator>() {
			private static final long serialVersionUID = 1L;
			public BoltSubOperator create() throws Exception {
				return new BoltSubOperator(bolt, routerTable);
			}
		});
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Operator.Router) {
			outputRouter.forward(msg, getContext());
			logger.info("Router updated.");
		} else if (msg instanceof InitPort) { // Init input router
            inputRouter.forward(msg, getContext());
            // The input router will notify the sender after initialized
		} unhandled(msg);
	}
}
