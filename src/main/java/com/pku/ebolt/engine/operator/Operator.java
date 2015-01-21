package com.pku.ebolt.engine.operator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.pku.ebolt.api.EBolt;
import com.pku.ebolt.engine.cluster.ResourceManager;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.AddressFromURIString;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.remote.RemoteScope;

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
    
    private EBolt appBolt;
    private String id;
	private Map<String, ActorRef> targets;
	private Map<String, RouteTree> targetRouters;
	private Map<String, ActorRef> sources;
	
	private RouteTree router;
	private Map<ActorRef, Integer> subOperatorRanges;
	
	public static Props props(final String id, final EBolt appBolt, final ActorRef resourceManager) {
		// Pass empty targets in constructor
		return props(id, appBolt, resourceManager, new HashMap<String, ActorRef>());
	}
	
	public static Props props(final String id, final EBolt appBolt, final ActorRef resourceManager,
			final Map<String, ActorRef> targets) {
		return Props.create(new Creator<Operator>() {
			private static final long serialVersionUID = 1L;
			public Operator create() throws Exception {
				return new Operator(id, appBolt, resourceManager, targets);
			}
		});
	}
	
	/**
	 * Add target to operator.
	 * @param targetID
	 * @param target
	 * @param operator
	 * @param self
	 */
	public static void addTarget(String targetID, ActorRef target, ActorRef operator, ActorRef self) {
		operator.tell(new Target(targetID, target), self);
	}
	
	/**
	 * Remove target from operator.
	 * TODO The framework have the ability to remove target, try to make use of it later.
	 * TODO When to remove source?
	 * @param targetID
	 * @param operator
	 * @param self
	 */
	public static void removeTarget(String targetID, ActorRef operator, ActorRef self) {
		// Pass null to remove the target
		operator.tell(new Target(targetID, null), self);
	}
	
	Operator(String id, EBolt appBolt, ActorRef resourceManager, Map<String, ActorRef> targets) {
		this.id = id;
		this.appBolt = appBolt;
		this.targets = new HashMap<String, ActorRef>();
		this.sources = new HashMap<String, ActorRef>();
		this.subOperatorRanges = new HashMap<ActorRef, Integer>();
		
		// Request initial resource
		// [WARNING] Block here!
		ResourceManager.Resources resources = null;
		try {
			resources =
				ResourceManager.requestResource(appBolt.INITIAL_CONCURRENCY, resourceManager, getSelf());
		} catch (Exception e) {
			System.err.println(e.toString());
		}
		assert(resources != null); // It means resources are not available.
		
		List<String> nodes = resources.nodes;
		List<ActorRef> subOperators = new LinkedList<ActorRef>();
		for (String node : nodes) {
			// Start subOperator remotely
			Address address = AddressFromURIString.parse(node);
			ActorRef subOperator = getContext().actorOf(SubOperator.props(this.appBolt, getSelf(), targetRouters)
					.withDeploy(new Deploy(new RemoteScope(address))));
			subOperators.add(subOperator);
		}
		router = new RouteTree(subOperators);
		subOperatorRanges = router.createTargetRanges();
		
		// Register on Targets and Request Routers from Targets
		for (Entry<String, ActorRef> targetEntry : targets.entrySet()) {
			ActorRef target = targetEntry.getValue();
			this.targets.put(targetEntry.getKey(), target);
			target.tell(new RouterRequest(id), getSelf());
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
				for (ActorRef subOperator : subOperatorRanges.keySet())
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
			for (ActorRef subOperator : subOperatorRanges.keySet())
				subOperator.forward(msg, getContext());
		} else unhandled(msg);
		// TODO Dynamic merging and divide sub-operators
	}
}
