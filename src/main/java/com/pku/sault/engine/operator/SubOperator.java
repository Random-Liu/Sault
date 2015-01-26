package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.Map;

import com.pku.sault.api.Bolt;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.pku.sault.engine.util.Logger;

class SubOperator extends UntypedActor {

    static class Port implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final ActorRef port;
        Port(ActorRef port) {
        	this.port = port;
        }
    }
	
    private ActorRef manager;
	private ActorRef inputRouter;
	private ActorRef outputRouter;
	private WorkerFactory workerFactory;
	private Logger logger;

	SubOperator(Bolt appBolt, ActorRef manager, Map<String, RouteTree> routerTable) {
		this.outputRouter = getContext().actorOf(OutputRouter.props(routerTable));
		this.workerFactory = new WorkerFactory(appBolt, outputRouter);
		this.inputRouter = getContext().actorOf(InputRouter.props(workerFactory));
		this.manager = manager;
		this.logger = new Logger(Logger.Role.SUB_OPERATOR);
		
		// Register Sub Operator Port to Manager
		this.manager.tell(new Port(this.inputRouter), getSelf());
		logger.info("Started.");
	}
	
	public static Props props(final Bolt appBolt, final ActorRef monitor, final Map<String, RouteTree> routerTable) {
		return Props.create(new Creator<SubOperator>() {
			private static final long serialVersionUID = 1L;
			public SubOperator create() throws Exception {
				return new SubOperator(appBolt, monitor, routerTable);
			}
		});
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Operator.Router) {
			logger.info("Router updated.");
			outputRouter.forward(msg, getContext());
		}
		else unhandled(msg);
	}
}
