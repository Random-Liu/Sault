package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import akka.pattern.Patterns;
import com.pku.sault.api.Task;
import com.pku.sault.engine.cluster.ResourceManager;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.remote.RemoteScope;
import com.pku.sault.engine.util.Constants;
import scala.concurrent.Await;
import scala.concurrent.Future;

import static akka.dispatch.Futures.sequence;

/**
 * @author taotaotheripper
 * In current version, send router between each other.
 * If this becomes a bottleneck, in next version, just send command
 * to change router between each other.
 */
//TODO Consider stopping operator while framework running later
public class Operator extends UntypedActor {
	static class Target implements Serializable {
		private static final long serialVersionUID = 1L;
		final String OperatorID;
		final ActorRef actor; // If target == null, it means remove the target
		Target(String OperatorID, ActorRef actor) {
			this.OperatorID = OperatorID;
			this.actor = actor;
		}
	}
	
	static class RouterRequest implements Serializable {
        private static final long serialVersionUID = 1L;
		final String OperatorID;
		RouterRequest(String OperatorID) {
        	this.OperatorID = OperatorID;
        }
	}
	
    static class Router implements Serializable {
        private static final long serialVersionUID = 1L;
        final String OperatorID;
        final RouteTree router;
        Router(String OperatorID, RouteTree router) {
        	this.OperatorID = OperatorID;
        	this.router = router;
        }
    }
    
    // TODO Change to class later!
    private Task task;
    private final String id;
	private Map<String, ActorRef> targets;
	private Map<String, RouteTree> targetRouters;
	private Map<String, ActorRef> sources;
	
	private RouteTree router;
	private Map<ActorRef, Integer> portRanges;
	private List<ActorRef> ports;
	private List<ActorRef> subOperators;

	private final ResourceManager resourceManager;
	private List<Address> resources;

	public static Props props(final String id, final Task task, final ResourceManager resourceManager) {
		// Pass empty targets in constructor
		return props(id, task, resourceManager, null);
	}
	
	public static Props props(final String id, final Task task, final ResourceManager resourceManager,
			final Map<String, ActorRef> targets) {
		return Props.create(new Creator<Operator>() {
			private static final long serialVersionUID = 1L;
			public Operator create() throws Exception {
				return new Operator(id, task, resourceManager, targets);
			}
		});
	}
	
	/**
	 * Add target to operator.
	 * @param targetID
	 * @param target
	 * @param operator
	 * @param context
	 */
	public static void addTarget(String targetID, ActorRef target, ActorRef operator, ActorContext context) {
		operator.tell(new Target(targetID, target), context.self());
	}
	
	/**
	 * Remove target from operator.
	 * TODO The framework have the ability to remove target, try to make use of it later.
	 * TODO When to remove source?
	 * @param targetID
	 * @param operator
	 * @param context
	 */
	public static void removeTarget(String targetID, ActorRef operator, ActorContext context) {
		// Pass null to remove the target
		operator.tell(new Target(targetID, null), context.self());
	}
	
	Operator(String id, Task task, ResourceManager resourceManager, Map<String, ActorRef> targets) {
		this.id = id;
		this.task = task;
		this.resourceManager = resourceManager;
		this.targets = new HashMap<String, ActorRef>();
		this.targetRouters = new HashMap<String, RouteTree>();
		this.sources = new HashMap<String, ActorRef>();
		this.router = null; // This will be initialized after subOperator started
		this.portRanges = null; // This will be initialized after subOperator started
		this.ports = new LinkedList<ActorRef>();
		this.subOperators = new LinkedList<ActorRef>();

		// The operator can only process message after initialized, so the following operations are blocking!
		// Request initial resource
		// [Caution] Block here!
		this.resources = this.resourceManager.allocateResource(task.PARALLELISM);
		assert(!this.resources.isEmpty()); // There must be resource to start the operator
		// Start sub-operators
		for (Address resource : this.resources) {
			// Start subOperator remotely
			ActorRef subOperator = getContext().actorOf(SubOperator.props(this.task, getSelf(), targetRouters)
					.withDeploy(new Deploy(new RemoteScope(resource))));
			subOperators.add(subOperator);
		}
		// Initializing router with port of sub-operators
		// [Caution] Block again here!
		List<Future<Object>> portFutures = new LinkedList<Future<Object>>();
		for (ActorRef subOperator : subOperators)
			portFutures.add(Patterns.ask(subOperator, SubOperator.PureMsg.PORT_REQUEST, Constants.futureTimeout));
		Future<Iterable<Object>> portsFuture = sequence(portFutures, getContext().dispatcher());
		try {
			Iterable<Object> portObjects = Await.result(portsFuture, Constants.futureTimeout.duration());
			for (Object port : portObjects)
				ports.add((ActorRef)port);
			router = new RouteTree(ports); // Initialize router
			portRanges = router.createTargetRanges(); // Initialize portRanges
		} catch (Exception e) {
			e.printStackTrace();
		}
		assert (router != null);
		assert (portRanges != null);

		// Register on Targets and Request Routers from Targets
		if (targets != null) { // If targets == null, it means that there are no initial targets.
			for (Entry<String, ActorRef> targetEntry : targets.entrySet()) {
				ActorRef target = targetEntry.getValue();
				this.targets.put(targetEntry.getKey(), target);
				target.tell(new RouterRequest(id), getSelf());
			}
		}
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Target) { // Add/Remove target
			Target target = (Target)msg;
			if (target.actor != null) { // Add target
				this.targets.put(target.OperatorID, target.actor);
				target.actor.tell(new RouterRequest(id), getSelf());
			} else { // Remove target
				// Remove local target info
				this.targets.remove(target.OperatorID);
				targetRouters.remove(target.OperatorID);
				// Broadcast null router to remove the target router on sub-operators
				for (ActorRef subOperator : subOperators)
					subOperator.tell(new Router(target.OperatorID, null), getSelf());
			}
		} else if (msg instanceof RouterRequest) {
			RouterRequest routerRequest = (RouterRequest)msg;
			// Register source operator
			this.sources.put(routerRequest.OperatorID, getSender());
			// Send ports back to source operator
			getSender().tell(new Router(id, router), getSelf());
		} else if (msg instanceof Router) {
			// Update local target info
			Router targetRouter = (Router)msg;
			assert(targets.containsKey(targetRouter.OperatorID)); // target should have been inserted
			targetRouters.put(targetRouter.OperatorID, targetRouter.router);
			// Broadcast new router to all sub operators
			for (ActorRef subOperator : subOperators)
				subOperator.forward(msg, getContext());
		} else unhandled(msg);
		// TODO Dynamic merging and divide sub-operators
	}
}
