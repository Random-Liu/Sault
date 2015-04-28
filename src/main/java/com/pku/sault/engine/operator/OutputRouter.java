package com.pku.sault.engine.operator;

import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import akka.japi.Creator;

import com.pku.sault.engine.util.Constants;
import com.pku.sault.engine.util.Logger;
import scala.concurrent.duration.Duration;

class OutputRouter extends UntypedActor {

	Map<String, RouteTree> routerTable;
	Map<String, HashMap<ActorRef, ActorRef>> bufferActors;

    // Just for experiment
    private boolean testing = true;
    private int receivedMessageNumber = 0;
    private Cancellable reportTimer;
    private final long reportInterval = 2;
    private final int REPORT = 0;
    private int round = 0;

	public static Props props(final Map<String, RouteTree> routerTable) {
		return Props.create(new Creator<OutputRouter>() {
			private static final long serialVersionUID = 1L;
			public OutputRouter create() throws Exception {
				return new OutputRouter(routerTable);
			}
		});
	}
	
	OutputRouter(Map<String, RouteTree> routerTable) {
		this.routerTable = routerTable;
		this.bufferActors = new HashMap<String, HashMap<ActorRef, ActorRef>>();
		for (String routerId : routerTable.keySet())
			bufferActors.put(routerId, new HashMap<ActorRef, ActorRef>());

        if (testing) {
            // Just for test
            reportTimer = getContext().system().scheduler().schedule(Duration.Zero(),
                    Duration.create(reportInterval, TimeUnit.SECONDS), getSelf(), REPORT,
                    getContext().system().dispatchers().lookup(Constants.TIMER_DISPATCHER), getSelf());
        }
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		// Update routerTable later
		if (msg instanceof TupleWrapper) {
            ++receivedMessageNumber;
			TupleWrapper tupleWrapper = (TupleWrapper)msg;
			for (Map.Entry<String, RouteTree> routerEntry : routerTable.entrySet()) {
				String routerId = routerEntry.getKey();
				RouteTree router = routerEntry.getValue();
				assert(router != null);
				// track(tupleWrapper); // Just for test
				ActorRef target = router.route(tupleWrapper);
				Map<ActorRef, ActorRef> targetToBufferActor = bufferActors.get(routerId);
				ActorRef bufferActor = targetToBufferActor.get(target);
				if (bufferActor == null) {
					bufferActor = getContext().actorOf(BufferActor.props(target));
					targetToBufferActor.put(target, bufferActor);
				}
				bufferActor.forward(msg, getContext()); // Forward to buffer actor*/
				// target.forward(msg, getContext());
			}
		} else if (msg instanceof Operator.Router) { // Add/Remove router
			Operator.Router router = (Operator.Router)msg;
			if (router.router == null) {
				this.routerTable.remove(router.OperatorID);
				// Flush all the messages and stop buffer actor
				Map<ActorRef, ActorRef> targetToBufferActor = bufferActors.get(router.OperatorID);
				for (ActorRef bufferActor : targetToBufferActor.values())
					getContext().stop(bufferActor);
				// Remove target to bufferActor map
				this.bufferActors.remove(router.OperatorID);
			} else {
				this.routerTable.put(router.OperatorID, router.router);
				// Create target to bufferActor map
				this.bufferActors.put(router.OperatorID, new HashMap<ActorRef, ActorRef>());
			}
		} else if (LatencyMonitor.isProbe(msg)) {
            getSender().forward(msg, getContext()); // Forward this back to the latency monitor
        } else if (msg.equals(REPORT)) {
            System.out.println("InputRate " + System.currentTimeMillis() / 1000 + " " + receivedMessageNumber / 2
                + " " + round);
            ++round;
            receivedMessageNumber = 0;
        } else unhandled(msg); // TODO Update Router
	}

	/*
	// For test
	long averageTime = 0;
	long count = 0;
	private void track(TupleWrapper tuple) {
		averageTime += System.currentTimeMillis() - (Long)((Tuple)(tuple.getTuple().getValue())).getKey();
		++count;
		if (count >= 10000) {
			System.out.println("Spout to Output Latency: " + (double) averageTime / count + " ms");
			averageTime = 0L;
			count = 0;
		}
	}
	*/
}

class BufferActor extends UntypedActor {
	static final int FLUSH = 0;
	static final long flushInterval = 2000000L; // 2 ms

	private final ActorRef target;
	private Queue<TupleWrapper> buffer;
	private long lastMsgTime = 0;
	private boolean isBuffering;
	private Cancellable timer;
	private Logger logger;

	/* For test */
	private boolean testing = false;
	private int blockNumber = 0;
	private int messageNumber = 0;
	private long lastTimeStamp = 0;
	private long reportInterval = 2; // 2s
	private Cancellable reportTimer;
	private static int REPORT = 1;


	public static Props props(final ActorRef target) {
		return Props.create(new Creator<BufferActor>() {
			private static final long serialVersionUID = 1L;
			public BufferActor create() throws Exception {
				return new BufferActor(target);
			}
		});
	}

	BufferActor(ActorRef target) {
		this.target = target;
		buffer = new LinkedList<TupleWrapper>();
		isBuffering = false; // Don't buffer at first
		logger = new Logger(Logger.Role.OUTPUT_ROUTER);

		if (testing) {
			// Enable this only when testing.
			this.reportTimer =
					timer = getContext().system().scheduler().schedule(Duration.Zero(),
							Duration.create(reportInterval, TimeUnit.SECONDS), getSelf(), REPORT,
							getContext().system().dispatchers().lookup(Constants.TIMER_DISPATCHER), getSelf());
			lastTimeStamp = System.currentTimeMillis();
		}
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof TupleWrapper) {
			long now = System.nanoTime(); //ns
			if (!isBuffering && now - lastMsgTime > flushInterval) {
				// System.out.println("Send directly!!!!!!!!!!!!!!!");
				// TODO If sender is useful, modify sender later, current now use output router as sender
				target.tell(msg, getContext().parent());
				++messageNumber;
				++blockNumber;
			} else {
				if (!isBuffering) {
					timer = getContext().system().scheduler().scheduleOnce(Duration.create(flushInterval, TimeUnit.NANOSECONDS),
							getSelf(), FLUSH, getContext().system().dispatchers().lookup(Constants.TIMER_DISPATCHER), getSelf());
					isBuffering = true;
				}
				buffer.offer((TupleWrapper) msg);
			}
			lastMsgTime = now;
		} else if (msg.equals(FLUSH)) {
			TupleWrapperBlock block = new TupleWrapperBlock(buffer);
			// System.out.println("Flushed " + buffer.size() + "messages in one block");
			target.tell(block, getContext().parent());
			messageNumber += buffer.size();
			++blockNumber;
			// Create new buffer, should not clear here, because the buffer is sent in block
			buffer = new LinkedList<TupleWrapper>();
			isBuffering = false;
		} else if (msg.equals(REPORT)) {
			long newTimeStamp = System.currentTimeMillis();
			logger.info("Send " + messageNumber + " messages in " + blockNumber + " blocks to " + target.path()
					+ " in " + ((double)(newTimeStamp - lastTimeStamp)/1000) + " s");
			lastTimeStamp = newTimeStamp;
			blockNumber = 0;
			messageNumber = 0;
		} else unhandled(msg);
	}

	@Override
	public void postStop() {
		TupleWrapperBlock block = new TupleWrapperBlock(buffer);
		target.tell(block, getContext().parent());
		if (!timer.isCancelled()) timer.cancel();
		if (reportTimer != null && !reportTimer.isCancelled()) reportTimer.cancel();
	}
}
