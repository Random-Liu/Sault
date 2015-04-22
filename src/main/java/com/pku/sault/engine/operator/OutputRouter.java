package com.pku.sault.engine.operator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

import com.pku.sault.api.Tuple; // Just for test

class OutputRouter extends UntypedActor {
	Map<String, RouteTree> routerTable;
	Map<String, HashMap<ActorRef, LinkedList<TupleWrapper>>> messageBuffer;
	
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
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		// Update routerTable later
		if (msg instanceof TupleWrapper) {
			TupleWrapper tupleWrapper = (TupleWrapper)msg;
			for (RouteTree router : routerTable.values()) {
				assert(router != null);
				// track(tupleWrapper); // Just for test
				ActorRef target = router.route(tupleWrapper);
				target.forward(msg, getContext());
			}
		} else if (msg instanceof Operator.Router) { // Add/Remove router
			Operator.Router router = (Operator.Router)msg;
			if (router.router == null)
				this.routerTable.remove(router.OperatorID);
			else
				this.routerTable.put(router.OperatorID, router.router);
		} else if (LatencyMonitor.isProbe(msg)) {
            getSender().forward(msg, getContext()); // Forward this back to the latency monitor
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

