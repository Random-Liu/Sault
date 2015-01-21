package com.pku.ebolt.engine.operator;

import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

class OutputRouter extends UntypedActor {
	Map<String, RouteTree> routerTable;
	
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
				ActorRef target = router.route(tupleWrapper);
				target.forward(msg, getContext());
			}
		} else if (msg instanceof Operator.Router) { // Add/Remove router
			Operator.Router router = (Operator.Router)msg;
			if (router.router == null)
				this.routerTable.remove(router.OperatorID);
			else
				this.routerTable.put(router.OperatorID, router.router);
		} else unhandled(msg); // TODO Update Router
	}
}

