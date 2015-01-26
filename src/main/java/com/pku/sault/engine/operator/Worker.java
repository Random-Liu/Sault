package com.pku.sault.engine.operator;

import com.pku.sault.api.Bolt;
import com.pku.sault.api.Tuple;

import akka.actor.*;
import akka.japi.Creator;
import com.pku.sault.engine.util.Logger;

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
	private Bolt bolt;
	private Logger logger;
	
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
		logger = new Logger(Logger.Role.WORKER);
		collector = new Collector(outputRouter, getSelf());
		bolt = appBolt;
		bolt.prepare(collector);
		logger.info("Started");
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Tuple) {
			bolt.execute((Tuple) msg);
			// TODO Flush every execution current now, can be optimized later. 
			collector.flush();
		} else unhandled(msg);
	}
	
	@Override
	public void postStop() {
		bolt.cleanup();
		collector.flush(); // In case that application emit some message in cleanup stage.
		logger.info("Stopped.");
	}
}
