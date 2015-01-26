package com.pku.sault.engine.operator;

import com.pku.sault.api.Bolt;
import com.pku.sault.api.Tuple;

import akka.actor.*;
import akka.japi.Creator;

class WorkerFactory {
	private Bolt bolt;
	private ActorRef outputRouter;
	private ActorContext context;
	
	// Add configuration later
	WorkerFactory(Bolt appBolt, ActorRef outputRouter) {
		this.bolt = appBolt;
		this.outputRouter = outputRouter;
	}
	
	void setContext(ActorContext context) {
		this.context = context;
	}
	
	ActorRef createWorker() {
		assert(context != null);
		return context.actorOf(Worker.props(this.bolt, this.outputRouter));
	}
}

class Worker extends UntypedActor {
	private Collector collector;
	private Bolt ebolt;
	
	public static Props props(final Bolt appBolt, final ActorRef outputRouter) {
		return Props.create(new Creator<Worker>() {
			private static final long serialVersionUID = 1L;
			public Worker create() throws Exception {
				return new Worker(appBolt, outputRouter);
			}
		});
	}
	
	// TODO Add configuration later
	Worker(Bolt appBolt, ActorRef outputRouter) {
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
