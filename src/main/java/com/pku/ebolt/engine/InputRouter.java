package com.pku.ebolt.engine;

import java.util.HashMap;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

class RouterMap {
	private HashMap<Object, ActorRef> routerTable;
	
	ActorRef route(TupleWrapper tupleWrapper) {
		return routerTable.get(tupleWrapper);
	}
	
	void addTarget(TupleWrapper tupleWrapper, ActorRef target) {
		assert(routerTable.containsKey(tupleWrapper));
	}
}

class InputRouter extends UntypedActor {

	private WorkerFactory workerFactory;
	
	public static Props props(final WorkerFactory workerFactory) {
		return Props.create(new Creator<InputRouter>() {
			private static final long serialVersionUID = 1L;
			public InputRouter create() throws Exception {
				return new InputRouter(workerFactory);
			}
		});
	}
	
	InputRouter(WorkerFactory workerFactory) {
		this.workerFactory = workerFactory;
		this.workerFactory.setContext(getContext());
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof TupleWrapper) {
			
		} else unhandled(msg);
	}
}
