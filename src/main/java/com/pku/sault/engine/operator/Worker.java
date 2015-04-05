package com.pku.sault.engine.operator;

import akka.japi.Procedure;
import com.pku.sault.api.Bolt;
import com.pku.sault.api.Spout;

import akka.actor.*;
import akka.japi.Creator;
import com.pku.sault.engine.util.Logger;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/*
class BoltWorkerFactory {
    Bolt bolt;
    ActorRef outputRouter;
    BoltWorkerFactory(Bolt bolt, ActorRef outputRouter) {
        this.bolt = bolt;
        this.outputRouter = outputRouter;
    }

    Props worker(KeyWrapper key) {
        return BoltWorker.props(key, bolt, outputRouter, null)/*.withDispatcher("sault-dispatcher");
    }
}
*/

class BoltWorker extends UntypedActor {
    // Current now, Get is sent by the input router, because it knows the original port
    // and original bolt better than the new bolt.
    static class TakeOver implements Serializable {
        private static final long serialVersionUID = 1L;
        final KeyWrapper key;
        TakeOver(KeyWrapper key) {
            this.key = key;
        }
    }

    static class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final Object state;
        State(Object state) {
            this.state = state;
        }
    }

    // TODO Make sure the buffer should not be too long (In fact if too long, we can do nothing)
    private class MessageBuffer { // Used for blocking messages
        private class BufferElement {
            final Object msg;
            final ActorRef sender;
            BufferElement(Object msg, ActorRef sender) {
                this.msg = msg;
                this.sender = sender;
            }
        }
        LinkedList<BufferElement> buffer = new LinkedList<BufferElement>();

        void buffer(Object msg, ActorRef sender) {
            buffer.push(new BufferElement(msg, sender));
        }

        void flush(ActorRef target) {
            for (BufferElement bufferElement : buffer)
                target.tell(bufferElement.msg, bufferElement.sender);
            buffer.clear();
        }
    }

    private KeyWrapper key;
	private Collector collector;
    private ActorRef outputRouter; // Used when forwarding probe
	private Bolt bolt;
	private Logger logger;
    private MessageBuffer messageBuffer;

	static Props props(final KeyWrapper key, final Bolt bolt, final ActorRef outputRouter/*, final ActorRef originalPort*/) {
		return Props.create(new Creator<BoltWorker>() {
			private static final long serialVersionUID = 1L;
			public BoltWorker create() throws Exception {
                return new BoltWorker(key, bolt, outputRouter/*, originalPort*/);
			}
		});
	}

	BoltWorker(KeyWrapper key, Bolt boltTemplate, ActorRef outputRouter/*, ActorRef originalPort*/) {
		this.key = key;
        logger = new Logger(Logger.Role.WORKER);
		collector = new Collector(outputRouter, getSelf());
        this.outputRouter = outputRouter;
		// Use copied bolt, so that worker will not affect each other
		try {
			bolt = boltTemplate.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		assert bolt != null;
        bolt.prepare(collector);
		logger.info("Bolt Started on " + getContext().system().name());

        /*
        if (originalPort != null) { // The bolt should take over original bolt
            logger.info("Bolt Start Migrating From " + originalPort);
            originalPort.tell(new TakeOver(this.key), getSelf());
            messageBuffer = new MessageBuffer();
            getContext().become(MIGRATING);
        }
        */
	}

    /*
    private Procedure<Object> MIGRATING = new Procedure<Object>() {
        @Override
        public void apply(Object msg) {
            // Loop and wait for original state
            if (msg instanceof State) {
                bolt.set(((State) msg).state);
                messageBuffer.flush(getSelf());
                getContext().unbecome(); // Finish migrating
                logger.info("Bolt State Set");
            } else if (LatencyMonitor.isProbe(msg)) { // Probe should never be blocked
                outputRouter.forward(msg, getContext());
            } else // Blocking the message in the buffer, and they will be processed after migrating.
                messageBuffer.buffer(msg, getSender());
        }
    };
    */

	@Override
	public void onReceive(Object msg) throws Exception {
        if (msg instanceof TupleWrapper) {
            assert ((TupleWrapper) msg).getKey().equals(key); // Just make sure
            bolt.execute(((TupleWrapper) msg).getTuple());
            // TODO Flush every execution current now, can be optimized later.
            collector.flush();
        } /*else if (msg instanceof TakeOver) {
            logger.info("Bolt Start Migrating to " + getSender());
            Object state = bolt.get();
            getSender().tell(new State(state), getSelf()); // Transfer state to the new bolt
            getContext().stop(getSelf()); // Stop itself
        }*/ else if (LatencyMonitor.isProbe(msg)) { // Forward the probe to outputRouter directly
            // In fact, if the collector is not flushed each execution, the probe should also be
            // sent with the collector. Current now, we just forward it directly.
            outputRouter.forward(msg, getContext());
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

    private final String EMIT = "EMIT";

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
        logger.info("Spout Started on " + getContext().system().name());
		getSelf().tell(EMIT, self()); // Start emitting
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg.equals(EMIT)) {
			long Timeout = spout.nextTuple();
			// TODO Flush every execution current now, can be optimized later.
			collector.flush();
			scheduler = getContext().system().scheduler().scheduleOnce(Duration.create(Timeout, TimeUnit.MICROSECONDS),
					getSelf(), EMIT, /*getContext().system().dispatchers().lookup("sault-dispatcher")*/getContext().dispatcher(), getSelf());
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
