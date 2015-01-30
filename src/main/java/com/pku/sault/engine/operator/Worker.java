package com.pku.sault.engine.operator;

import com.pku.sault.api.Bolt;
import com.pku.sault.api.Spout;

import akka.actor.*;
import akka.japi.Creator;
import com.pku.sault.engine.util.Logger;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

class BoltWorker extends UntypedActor {
	private Collector collector;
	private Bolt bolt;
	private Logger logger;

	static Props props(final Bolt bolt, final ActorRef outputRouter) {
		return Props.create(new Creator<BoltWorker>() {
			private static final long serialVersionUID = 1L;
			public BoltWorker create() throws Exception {
				return new BoltWorker(bolt, outputRouter);
			}
		});
	}

	BoltWorker(Bolt boltTemplate, ActorRef outputRouter) {
		logger = new Logger(Logger.Role.WORKER);
		collector = new Collector(outputRouter, getSelf());
		// Use copied bolt, so that worker will not affect each other
		try {
			bolt = boltTemplate.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		assert bolt != null;
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

	static Props props(final Spout spout, final ActorRef outputRouter) {
		return Props.create(new Creator<SpoutWorker>() {
			private static final long serialVersionUID = 1L;
			public SpoutWorker create() throws Exception {
				return new SpoutWorker(spout, outputRouter);
			}
		});
	}

	private enum SpoutCmd {
		EMIT
	}

	SpoutWorker(Spout spoutTemplate, ActorRef outputRouter) {
		logger = new Logger(Logger.Role.WORKER);
		collector = new Collector(outputRouter, getSelf());
		// Use copied spout, so that worker will not affect each other
		try {
			spout = spoutTemplate.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		assert spout != null;
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
