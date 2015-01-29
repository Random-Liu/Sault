package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.pku.sault.api.Task;
import com.pku.sault.engine.util.Logger;

class SubOperator extends UntypedActor {
	static enum PureMsg {
		PORT_REQUEST
	} // Add more if needed
	
    private ActorRef manager;
	private ActorRef inputRouter;
	private ActorRef outputRouter;
	private Logger logger;

	SubOperator(Task task, ActorRef manager, Map<String, RouteTree> routerTable) {
		this.outputRouter = getContext().actorOf(OutputRouter.props(routerTable));
		this.inputRouter = getContext().actorOf(InputRouter.props(task, outputRouter));
		this.manager = manager;
		this.logger = new Logger(Logger.Role.SUB_OPERATOR);

		logger.info("Started.");
	}

	static Props props(final Task task, final ActorRef manager, final Map<String, RouteTree> routerTable) {
		return Props.create(new Creator<SubOperator>() {
			private static final long serialVersionUID = 1L;
			public SubOperator create() throws Exception {
				return new SubOperator(task, manager, routerTable);
			}
		});
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Operator.Router) {
			outputRouter.forward(msg, getContext());
			logger.info("Router updated.");
		} else if (msg == PureMsg.PORT_REQUEST) {
			getSender().tell(inputRouter, getSelf()); // Return ActorRef directly
			logger.info("Input router registered");
		} unhandled(msg);
	}
}
