package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import akka.japi.Pair;
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
import com.pku.sault.engine.util.Logger;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
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
    public static enum Test {
        SPLIT,
        MERGE
        // Add more later
    }

    static class Split implements Serializable {
        private static final long serialVersionUID = 1L;
        final ActorRef subOperator;
        Split(ActorRef subOperator) {
            this.subOperator = subOperator;
        }
    }

    static class Merge implements Serializable {
        private static final long serialVersionUID = 1L;
        final ActorRef subOperator;
        Merge(ActorRef subOperator) {
            this.subOperator = subOperator;
        }
    }

    private class SubOperatorInfo {
        int lowerBound;
        Address resource;
        ActorRef port;
        SubOperatorInfo(int lowerBound, Address resource, ActorRef port) {
            this.lowerBound = lowerBound;
            this.resource = resource;
            this.port = port;
        }
    }
    // TODO Change to class later!
    private Bolt bolt;
    private final String id;
	private Map<String, ActorRef> targets;
	private Map<String, RouteTree> targetRouters;
	private Map<String, ActorRef> sources;
	
	private RouteTree router;
    private Map<ActorRef, SubOperatorInfo> subOperatorsInfo;

	private final ResourceManager resourceManager;
	private List<Address> resources;

    private Address latencyMonitorActorSystem;
    private ActorRef latencyMonitor;

    private Logger logger;

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
		this.router = new RouteTree();
        this.subOperatorsInfo = new HashMap<ActorRef, SubOperatorInfo>();
        this.logger = new Logger(Logger.Role.SUB_OPERATOR);

		// The operator can only process message after initialized, so the following operations are blocking!
		// Request initial resource
		// [Caution] Block here!
		this.resources = this.resourceManager.allocateResource(bolt.getInitialParallelism());
		assert(!this.resources.isEmpty()); // There must be resource to start the operator

        LinkedList<ActorRef> subOperators = new LinkedList<ActorRef>();
		// Start sub-operators
		for (Address resource : this.resources) {
			// Start subOperator remotely
            // Pass target routers will cause shared memory
			ActorRef subOperator = getContext().actorOf(BoltSubOperator.props(this.bolt, /*targetRouters*/
                    new HashMap<String, RouteTree>())
					.withDeploy(new Deploy(new RemoteScope(resource))));
			subOperators.add(subOperator);
		}

        // Create empty cells in route tree
        // create empty cell according to the real resource number
        List<Integer> lowerBounds = router.createEmptyCells(resources.size());

		// Initializing router with port of sub-operators
		// [Caution] Block again here!
		List<Future<Object>> portFutures = new LinkedList<Future<Object>>();

        for (int subOperatorIndex = 0; subOperatorIndex < subOperators.size(); ++subOperatorIndex) {
            // int lowerBound = lowerBounds.get(subOperatorIndex);
            // int upperBound = router.getUpperBound(lowerBound);
            ActorRef subOperator = subOperators.get(subOperatorIndex);
            portFutures.add(Patterns.ask(subOperator, BoltSubOperator.PORT_PLEASE, Constants.futureTimeout));
        }
		Future<Iterable<Object>> portsFuture = sequence(portFutures, getContext().dispatcher());
		try {
			Iterable<Object> portObjects = Await.result(portsFuture, Constants.futureTimeout.duration());
            int portIndex = 0;
			for (Object portObject : portObjects) {
                ActorRef port = (ActorRef) portObject;
                int lowerBound = lowerBounds.get(portIndex);
                Address resource = resources.get(portIndex);
                subOperatorsInfo.put(subOperators.get(portIndex), new SubOperatorInfo(lowerBound,
                        resource, port));
                router.setTarget(lowerBound, port); // Fill the empty cell
                ++portIndex;
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
		assert (router != null);

        // Initialize latency monitor in a standalone resource
        List<Pair<ActorRef, ActorRef>> targetPorts = new LinkedList<Pair<ActorRef, ActorRef>>();
        for (Entry<ActorRef, SubOperatorInfo> subOperatorInfoEntry : subOperatorsInfo.entrySet())
            targetPorts.add(new Pair<ActorRef, ActorRef>(subOperatorInfoEntry.getKey(), subOperatorInfoEntry.getValue().port));

        // This is the only way I can come up with now.
        this.latencyMonitorActorSystem = this.resourceManager.allocateLocalResource("LatencyMonitor-"+id);
        this.latencyMonitor = getContext().actorOf(LatencyMonitor.props(targetPorts, bolt)
           .withDeploy(new Deploy(new RemoteScope(latencyMonitorActorSystem))));

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
				for (ActorRef subOperator : subOperatorsInfo.keySet())
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
			for (ActorRef subOperator : subOperatorsInfo.keySet())
				subOperator.forward(msg, getContext());
		} else if (msg instanceof Split) {
			logger.info("Start splitting");
            // Number of sub operator should never exceed max parallelism.
            // TODO: Return here
            assert bolt.getMaxParallelism() > subOperatorsInfo.size();
            // 1. Create new sub operator.
            // 2. Update upstream router.
            Split split = (Split)msg;
            ActorRef originalSubOperator = split.subOperator;
            SubOperatorInfo originalSubOperatorInfo = subOperatorsInfo.get(originalSubOperator);
            // ActorRef originalPort = originalSubOperatorInfo.port;
            int originalLowerBound = originalSubOperatorInfo.lowerBound;

            List<Address> nodes = resourceManager.allocateResource(1);
            if (nodes.isEmpty()) { // There is no more resources, or meet an error during resource allocating
                logger.warn("Allocating resource error when elastic scaling.");
                return;
            }
            assert nodes.size() == 1;
            Address node = nodes.get(0);
            ActorRef newSubOperator = getContext().actorOf(BoltSubOperator.props(bolt, cloneTargetRouters())
                    .withDeploy(new Deploy(new RemoteScope(node))));
            int newLowerBound = router.split(originalLowerBound, null); // Fill this cell later
            // int upperBound = router.getUpperBound(newLowerBound);
            Future<Object> newPortFuture = Patterns.ask(newSubOperator, BoltSubOperator.PORT_PLEASE, Constants.futureTimeout);
            ActorRef newPort = (ActorRef)Await.result(newPortFuture, Constants.futureTimeout.duration());
            router.setTarget(newLowerBound, newPort);
            subOperatorsInfo.put(newSubOperator, new SubOperatorInfo(newLowerBound, node, newPort));

            // TODO: If we split according to resource usage, we should never start a latency monitor.
            LatencyMonitor.done(latencyMonitor, getContext()); // Tell latency monitor that splitting is done.
            // Because DONE message is sent first, the latency monitor must be in working state now.
            LatencyMonitor.addTarget(latencyMonitor, newSubOperator, newPort, getContext()); // Start monitoring new subOperator

            updateUpstreamRouter();
        } else if (msg instanceof Merge) {
            logger.info("Start merging");
            // 1. Update upstream router
            // 2. TODO: Stop useless sub operator
            //    Current now, don't stop it. Because it should finish processing the tuples already sent to it.
            //    If we add ack server later, we can stop it immediately.
            Merge merge = (Merge)msg;
            ActorRef mergedSubOperator = merge.subOperator;
            SubOperatorInfo mergedSubOperatorInfo = subOperatorsInfo.get(mergedSubOperator);
            int mergedLowerBound = mergedSubOperatorInfo.lowerBound;

            if (router.isSiblingAvailable(mergedLowerBound)) { // The sub operator can be merged
                ActorRef siblingSubOperatorPort = router.sibling(mergedLowerBound);
                ActorRef siblingSubOperator = null;
                for (Entry<ActorRef, SubOperatorInfo> subOperatorInfoEntry : subOperatorsInfo.entrySet()) {
                    // Just traverse current now
                    if (subOperatorInfoEntry.getValue().port.equals(siblingSubOperatorPort)) {
                        siblingSubOperator = subOperatorInfoEntry.getKey();
                        break;
                    }
                }
                assert (siblingSubOperator != null);
                SubOperatorInfo siblingSubOperatorInfo = subOperatorsInfo.get(siblingSubOperator);
                siblingSubOperatorInfo.lowerBound = router.merge(mergedLowerBound);
                logger.info("Sub operator is merged");
            } else { // If the sub operator can not be merged, just relocate it
                List<Address> nodes = resourceManager.allocateResource(1); // The resource manager will ensure that the
                // resource which will be released will not been allocated any more.
                if (nodes.isEmpty()) { // There is no more resources, or meet an error during resource allocating
                    logger.warn("Allocating resource error when elastic scaling.");
                    return;
                }
                assert nodes.size() == 1;
                Address node = nodes.get(0);
                ActorRef newSubOperator = getContext().actorOf(BoltSubOperator.props(bolt, cloneTargetRouters())
                        .withDeploy(new Deploy(new RemoteScope(node))));
                Future<Object> newPortFuture = Patterns.ask(newSubOperator, BoltSubOperator.PORT_PLEASE, Constants.futureTimeout);
                ActorRef newPort = (ActorRef)Await.result(newPortFuture, Constants.futureTimeout.duration());
                router.setTarget(mergedLowerBound, newPort);
                subOperatorsInfo.put(newSubOperator, new SubOperatorInfo(mergedLowerBound, node, newPort));
                LatencyMonitor.addTarget(latencyMonitor, mergedSubOperator, newPort, getContext()); // Start monitoring new subOperator
                logger.info("Sub operator is relocated");
            }

            // The merge request should be invoked by the resource manager
            LatencyMonitor.removeTarget(latencyMonitor, mergedSubOperator, getContext()); // Remove merged sub-operator
            subOperatorsInfo.remove(mergedSubOperator);
            updateUpstreamRouter();
        } else if (msg instanceof Test) {
            doTest((Test) msg); // Only used for test
        } else unhandled(msg);
		// TODO Dynamic merging sub-operators
	}

    private void updateUpstreamRouter() {
        for (ActorRef source : sources.values())
            source.tell(new Operator.Router(id, router), getSelf()); // Update router of all sources
    }

    private HashMap<String, RouteTree> cloneTargetRouters() {
        HashMap<String, RouteTree> newTargetRouters = new HashMap<String, RouteTree>();
        for (Entry<String, RouteTree> targetRouterEntry : targetRouters.entrySet())
            newTargetRouters.put(targetRouterEntry.getKey(), targetRouterEntry.getValue());
        // Because router can only be modified by the operator, so we do not need to clone it.
        return newTargetRouters;
    }

    private void doTest(Test test) {
        switch (test) {
            case SPLIT: { // Test split, split the first subOperator
                assert !subOperatorsInfo.isEmpty();
                Iterator<ActorRef> subOperatorIterator = subOperatorsInfo.keySet().iterator();
                ActorRef subOperator = subOperatorIterator.next();
                getSelf().tell(new Split(subOperator), getSelf());
                break;
            }
            case MERGE: {
                assert !subOperatorsInfo.isEmpty();
                Iterator<ActorRef> subOperatorIterator = subOperatorsInfo.keySet().iterator();
                ActorRef subOperator = subOperatorIterator.next();
                getSelf().tell(new Merge(subOperator), getSelf());
            }
            default: break;
        }
    }
}
