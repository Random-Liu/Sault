package com.pku.sault.engine.operator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import akka.pattern.Patterns;
import com.pku.sault.api.Bolt;
import com.pku.sault.engine.cluster.ResourceManager;

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
public class BoltOperator extends UntypedActor {
    
    // TODO Change to class later!
    private Bolt bolt;
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

	public static Props props(final String id, final Bolt bolt, final ResourceManager resourceManager) {
		// Pass empty targets in constructor
		return props(id, bolt, resourceManager, null);
	}
	
	public static Props props(final String id, final Bolt bolt, final ResourceManager resourceManager,
			final Map<String, ActorRef> targets) {
		return Props.create(new Creator<BoltOperator>() {
			private static final long serialVersionUID = 1L;
			public BoltOperator create() throws Exception {
				return new BoltOperator(id, bolt, resourceManager, targets);
			}
		});
	}
	
	BoltOperator(String id, Bolt bolt, ResourceManager resourceManager, Map<String, ActorRef> targets) {
		this.id = id;
		this.bolt = bolt;
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
		this.resources = this.resourceManager.allocateResource(bolt.getInitialParallelism());
		assert(!this.resources.isEmpty()); // There must be resource to start the operator
		// Start sub-operators
		for (Address resource : this.resources) {
			// Start subOperator remotely
			ActorRef subOperator = getContext().actorOf(BoltSubOperator.props(this.bolt, targetRouters)
					.withDeploy(new Deploy(new RemoteScope(resource))));
			subOperators.add(subOperator);
		}
		// Initializing router with port of sub-operators
		// [Caution] Block again here!
		List<Future<Object>> portFutures = new LinkedList<Future<Object>>();
		for (ActorRef subOperator : subOperators)
			portFutures.add(Patterns.ask(subOperator, BoltSubOperator.PureMsg.PORT_REQUEST, Constants.futureTimeout));
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
				target.tell(new Operator.RouterRequest(id), getSelf());
			}
		}
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Operator.Target) { // Add/Remove target
			Operator.Target target = (Operator.Target)msg;
			if (target.actor != null) { // Add target
				this.targets.put(target.OperatorID, target.actor);
				target.actor.tell(new Operator.RouterRequest(id), getSelf());
			} else { // Remove target
				// Remove local target info
				this.targets.remove(target.OperatorID);
				targetRouters.remove(target.OperatorID);
				// Broadcast null router to remove the target router on sub-operators
				for (ActorRef subOperator : subOperators)
					subOperator.tell(new Operator.Router(target.OperatorID, null), getSelf());
			}
		} else if (msg instanceof Operator.RouterRequest) {
			Operator.RouterRequest routerRequest = (Operator.RouterRequest)msg;
			// Register source operator
			this.sources.put(routerRequest.OperatorID, getSender());
			// Send ports back to source operator
			getSender().tell(new Operator.Router(id, router), getSelf());
		} else if (msg instanceof Operator.Router) {
			// Update local target info
			Operator.Router targetRouter = (Operator.Router)msg;
			assert(targets.containsKey(targetRouter.OperatorID)); // target should have been inserted
			targetRouters.put(targetRouter.OperatorID, targetRouter.router);
			// Broadcast new router to all sub operators
			for (ActorRef subOperator : subOperators)
				subOperator.forward(msg, getContext());
		} else unhandled(msg);
		// TODO Dynamic merging and divide sub-operators
	}
}
