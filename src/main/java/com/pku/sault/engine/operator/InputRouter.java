package com.pku.sault.engine.operator;

import java.util.HashMap;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

class RouterMap {
	private HashMap<Object, ActorRef> routerTable;
	
	RouterMap() {
		routerTable = new HashMap<Object, ActorRef> ();
	}
	
	boolean isTargetAvailable(TupleWrapper tupleWrapper) {
		return routerTable.get(tupleWrapper.getKey())  != null;
	}
	
	ActorRef route(TupleWrapper tupleWrapper) {
		return routerTable.get(tupleWrapper.getKey());
	}
	
	void setTarget(TupleWrapper tupleWrapper, ActorRef target) {
		routerTable.put(tupleWrapper.getKey(), target);
	}
}

class InputRouter extends UntypedActor {

	private WorkerFactory workerFactory;
	private RouterMap routerMap;
	
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
		this.routerMap = new RouterMap();
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof TupleWrapper) {
			TupleWrapper tupleWrapper = (TupleWrapper)msg;
			ActorRef target = routerMap.route(tupleWrapper);
			if (target == null) {
				target = workerFactory.createWorker();
				routerMap.setTarget(tupleWrapper, target);
			}
			target.forward(msg, getContext());
		} else unhandled(msg);
	}
}
