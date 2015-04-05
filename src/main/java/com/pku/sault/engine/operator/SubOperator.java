package com.pku.sault.engine.operator;

import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.routing.BroadcastPool;
import com.pku.sault.api.Bolt;
import com.pku.sault.api.Spout;
import com.pku.sault.engine.util.Logger;

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
	static final String PORT_PLEASE = "port_please";

    private Bolt bolt;
    private ActorRef manager;
	private ActorRef inputRouter;
    // private ActorRef latencyMonitor;
	private ActorRef outputRouter;
	private Logger logger;

	// TODO: Add ack monitor
	BoltSubOperator(Bolt bolt, Map<String, RouteTree> routerTable) {
        this.bolt = bolt;
        this.manager = getContext().parent(); // This hasn't been used.
		// We have moved latencyMonitor to Operator
        this.outputRouter = getContext().actorOf(OutputRouter.props(routerTable));
        this.inputRouter = getContext().actorOf(InputRouter.props(bolt, outputRouter));
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
		} else if (msg.equals(PORT_PLEASE)) { // Init input router
			getSender().tell(inputRouter, getSelf());
            // inputRouter.forward(msg, getContext());
            // The input router will notify the sender after initialized
		} unhandled(msg);
	}
}
