package com.pku.sault.engine.operator;

import com.pku.sault.api.Bolt;
import com.pku.sault.api.Spout;
import com.pku.sault.api.Task;
import com.pku.sault.api.Tuple;

import akka.actor.*;
import akka.japi.Creator;
import com.pku.sault.engine.util.Logger;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

class Worker {
	// Factory function
	static Props props(Task task, ActorRef outputRouter) {
		assert (task instanceof Bolt || task instanceof Spout); // Just in case
		if (task instanceof Bolt) // Start different worker for different task
			return BoltWorker.props(task, outputRouter);
		else
			return SpoutWorker.props(task, outputRouter);
	}
}

class BoltWorker extends UntypedActor {
	private Collector collector;
	private Bolt bolt;
	private Logger logger;

	static Props props(final Task task, final ActorRef outputRouter) {
		return Props.create(new Creator<BoltWorker>() {
			private static final long serialVersionUID = 1L;
			public BoltWorker create() throws Exception {
				return new BoltWorker(task, outputRouter);
			}
		});
	}

	BoltWorker(Task task, ActorRef outputRouter) {
		assert(task instanceof Bolt); // Just ensure
		logger = new Logger(Logger.Role.WORKER);
		collector = new Collector(outputRouter, getSelf());
		// Use copied task, so that worker will not affect each other
		this.bolt = (Bolt)task.clone();
		bolt.prepare(collector);
		logger.info("Bolt Started");
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof TupleWrapper) {
			bolt.execute(((TupleWrapper) msg).getTuple());
			// TODO Flush every execution current now, can be optimized later.
			collector.flush();
		} else unhandled(msg);
	}
	
	@Override
	public void postStop() {
		bolt.cleanup();
		collector.flush(); // In case that application emit some message in cleanup stage.
		logger.info("Bolt Stopped.");
	}
}

class SpoutWorker extends UntypedActor {
	private Collector collector;
	private Spout spout;
	private Logger logger;
	private  Cancellable scheduler;

	static Props props(final Task task, final ActorRef outputRouter) {
		return Props.create(new Creator<SpoutWorker>() {
			private static final long serialVersionUID = 1L;
			public SpoutWorker create() throws Exception {
				return new SpoutWorker(task, outputRouter);
			}
		});
	}

	private enum SpoutCmd {
		EMIT
	}

	SpoutWorker(Task task, ActorRef outputRouter) {
		assert(task instanceof Spout); // Just ensure
		logger = new Logger(Logger.Role.WORKER);
		collector = new Collector(outputRouter, getSelf());
		// Use copied task, so that worker will not affect each other
		spout = (Spout)task.clone();
		spout.open(collector);
		logger.info("Spout Started");
		getSelf().tell(SpoutCmd.EMIT, self()); // Start emitting
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof  SpoutCmd) {
			long Timeout = spout.nextTuple();
			// TODO Flush every execution current now, can be optimized later.
			collector.flush();
			scheduler = getContext().system().scheduler().scheduleOnce(Duration.create(Timeout, TimeUnit.MILLISECONDS),
					getSelf(), SpoutCmd.EMIT, getContext().dispatcher(), getSelf());
		} else
			unhandled(msg);
	}

	@Override
	public void postStop() {
		scheduler.cancel(); // Cancel the scheduler
		spout.close();
		collector.flush(); // In case that application emit some message in close stage.
		logger.info("Spout Stopped.");
	}
}
