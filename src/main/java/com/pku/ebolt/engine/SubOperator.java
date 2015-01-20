package com.pku.ebolt.engine;

import java.io.Serializable;
import java.util.List;

import com.pku.ebolt.api.IEBolt;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

class SubOperator extends UntypedActor {

    static class Port implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final int subOperatorID;
        final ActorRef port;
        Port(int subOperatorID, ActorRef port) {
        	this.port = port;
        	this.subOperatorID = subOperatorID;
        }
    }
	
    private int id;
    private ActorRef monitor;
	private ActorRef inputRouter;
	private ActorRef outputRouter;
	private WorkerFactory workerFactory;
	
	SubOperator(int id, IEBolt appBolt, ActorRef monitor, List<ActorRef> targetRouters) {
		this.id = id;
		this.outputRouter = getContext().actorOf(OutputRouter.props(targetRouters));
		this.workerFactory = new WorkerFactory(appBolt, outputRouter);
		this.inputRouter = getContext().actorOf(InputRouter.props(workerFactory));
		this.monitor = monitor;
		
		// Register Sub Operator Port to Monitor
		this.monitor.tell(new Port(this.id, this.inputRouter), getSelf());
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		
	}
}
