package com.pku.sault.engine.operator;

import java.util.HashMap;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter;
import com.pku.sault.api.Task;

// TODO Make this configurable later
class InputRouter {
	// Factory function
	static Props props (Task task, ActorRef outputRouter) {
		Props workerProps = Worker.props(task, outputRouter);
		if (task.INSTANCE_NUMBER == 0)
			return DynamicInputRouter.props(workerProps);
		else
			return StaticHashRouter.props(task.INSTANCE_NUMBER, workerProps);
	}
}

// Hash router with fixed instance number
class StaticHashRouter {
	// Although the api is deprecated, this is the best way to impelement this function
	// TODO Remove this deprecated api.
	private static final ConsistentHashingRouter.ConsistentHashMapper hashMapper = new ConsistentHashingRouter.ConsistentHashMapper() {
		@Override
		public Object hashKey(Object msg) {
			if (msg instanceof TupleWrapper)
				return ((TupleWrapper) msg).getKey();
			else
				return null;
		}
	};

	static Props props(int instanceNumber, Props workerProps) {
		return new ConsistentHashingPool(instanceNumber).props(workerProps);
	}
}

// Hash router with dynamic changing instance number
class DynamicInputRouter extends UntypedActor {

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

	private Props workerProps;
	private RouterMap routerMap;
	
	public static Props props(final Props workerProps) {
		return Props.create(new Creator<DynamicInputRouter>() {
			private static final long serialVersionUID = 1L;
			public DynamicInputRouter create() throws Exception {
				return new DynamicInputRouter(workerProps);
			}
		});
	}

	DynamicInputRouter(Props workerProps) {
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