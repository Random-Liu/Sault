package com.pku.sault.engine.operator;

import java.util.HashMap;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

// Hash router with dynamic changing instance number
class InputRouter extends UntypedActor {

	class RouterMap {
		private HashMap<KeyWrapper, ActorRef> routerTable;

		RouterMap() {
			routerTable = new HashMap<KeyWrapper, ActorRef> ();
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

	private Props workerProps;
	private RouterMap routerMap;
	
	public static Props props(final Props workerProps) {
		return Props.create(new Creator<InputRouter>() {
			private static final long serialVersionUID = 1L;
			public InputRouter create() throws Exception {
				return new InputRouter(workerProps);
			}
		});
	}

	InputRouter(Props workerProps) {
		this.workerProps = workerProps;
		this.routerMap = new RouterMap();
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof TupleWrapper) {
			TupleWrapper tupleWrapper = (TupleWrapper)msg;
			ActorRef target = routerMap.route(tupleWrapper);
			if (target == null) {
				target = getContext().actorOf(workerProps);
				routerMap.setTarget(tupleWrapper, target);
			}
			target.forward(msg, getContext());
		} else unhandled(msg);
	}
}