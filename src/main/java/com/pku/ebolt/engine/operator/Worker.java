package com.pku.ebolt.engine.operator;

import com.pku.ebolt.api.EBolt;
import com.pku.ebolt.api.Tuple;

import akka.actor.*;
import akka.japi.Creator;

class WorkerFactory {
	private EBolt ebolt;
	private ActorRef outputRouter;
	private ActorContext context;
	
	// Add configuration later
	WorkerFactory(EBolt appBolt, ActorRef outputRouter) {
		this.ebolt = appBolt;
		this.outputRouter = outputRouter;
	}
	
	void setContext(ActorContext context) {
		this.context = context;
	}
	
	ActorRef createWorker() {
		assert(context != null);
		return context.actorOf(Worker.props(this.ebolt, this.outputRouter));
	}
}

class Worker extends UntypedActor {
	private Collector collector;
	private EBolt ebolt;
	
	public static Props props(final EBolt appBolt, final ActorRef outputRouter) {
		return Props.create(new Creator<Worker>() {
			private static final long serialVersionUID = 1L;
			public Worker create() throws Exception {
				return new Worker(appBolt, outputRouter);
			}
		});
	}
	
	// TODO Add configuration later
	Worker(EBolt appBolt, ActorRef outputRouter) {
		collector = new Collector(outputRouter, getSelf());
		ebolt = appBolt;
		ebolt.prepare(collector);
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Tuple) {
			ebolt.execute((Tuple)msg);
			// TODO Flush every execution current now, can be optimized later. 
			collector.flush();
		} else unhandled(msg);
	}
	
	@Override
	public void postStop() {
		ebolt.cleanup();
		collector.flush(); // In case that application emit some message in cleanup stage.
	}
}
