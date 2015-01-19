package com.pku.ebolt.engine;

import java.util.Iterator;
import java.util.List;

import com.pku.ebolt.api.IEBolt;
import com.pku.ebolt.api.Tuple;

import akka.actor.*;
import akka.japi.Creator;

class WorkerFactory {
	private IEBolt ebolt;
	private ActorRef outputRouter;
	private ActorContext context;
	
	// Add configuration later
	WorkerFactory(IEBolt appBolt, ActorRef outputRouter) {
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
	private IEBolt ebolt;
	private ActorRef outputRouter;
	
	public static Props props(final IEBolt appBolt, final ActorRef outputRouter) {
		return Props.create(new Creator<Worker>() {
			private static final long serialVersionUID = 1L;
			public Worker create() throws Exception {
				return new Worker(appBolt, outputRouter);
			}
		});
	}
	
	// TODO Add configuration later
	Worker(IEBolt appBolt, ActorRef outputRouter) {
		this.outputRouter = outputRouter;
		
		collector = new Collector();
		ebolt = appBolt;
		ebolt.prepare(collector);
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Tuple) {
			ebolt.execute((Tuple)msg);
			Iterator<Tuple> tupleIter = collector.getBufferIterator();
			while (tupleIter.hasNext()) {
				outputRouter.tell(tupleIter.next(), getSelf());
			}
		} else unhandled(msg);
	}
	
	@Override
	public void postStop() {
		ebolt.cleanup();
	}
}
