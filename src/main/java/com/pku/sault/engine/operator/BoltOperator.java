package com.pku.sault.engine.operator;

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

        for (ActorRef subOperator : subOperators)
            portFutures.add(Patterns.ask(subOperator, BoltSubOperator.PORT_PLEASE, Constants.futureTimeout));

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
		} else if (msg instanceof LatencyMonitor.LoadStatus) { // Handle load status
            handleLoadStatus(((LatencyMonitor.LoadStatus) msg).loadStatus);
        } else if (msg instanceof Test) {
            doTest((Test) msg); // Only used for test
        } else unhandled(msg);
	}

    private void handleLoadStatus(Map<ActorRef, LatencyMonitor.LOAD_STATUS> targetsLoadStatus) {
        Set<ActorRef> doneTargets = new HashSet<ActorRef>();
        boolean needUpdate = false;
        for (Entry<ActorRef, LatencyMonitor.LOAD_STATUS> targetLoadStatusEntry : targetsLoadStatus.entrySet()) {
            ActorRef target = targetLoadStatusEntry.getKey();
            LatencyMonitor.LOAD_STATUS loadStatus = targetLoadStatusEntry.getValue();
            if (!doneTargets.contains(target)) {
                if (loadStatus == LatencyMonitor.LOAD_STATUS.OVERLOAD) // Split
                    needUpdate = needUpdate || doSplit(target);
                else if (loadStatus == LatencyMonitor.LOAD_STATUS.UNDERLOAD) { // Merge
                    // Select left or right
                    int lowerBound = subOperatorsInfo.get(target).lowerBound;
                    int leftOrRight = 0; // -1 means left, 1 means right, 0 means none
                    ActorRef leftSibling = portToTarget(router.leftSibling(lowerBound));
                    ActorRef rightSibling = portToTarget(router.rightSibling(lowerBound));
                    // 1. If sibling is done, it will not be used
                    // 2. The priority is UNDERLOAD > LOWLOAD > NORMALLOAD.
                    // HIGHLOAD and OVERLOAD will never be merged
                    // TODO Relocate low load target which can not be merged
                    boolean isLeftSiblingAvailable = canBeMerged(targetsLoadStatus, doneTargets, leftSibling);
                    boolean isRightSiblingAvailable = canBeMerged(targetsLoadStatus, doneTargets, rightSibling);
                    if (isLeftSiblingAvailable || isRightSiblingAvailable) {
                        if (!isLeftSiblingAvailable) leftOrRight = 1; // Select right
                        else if (!isRightSiblingAvailable) leftOrRight = -1; // Select left
                        else {
                            LatencyMonitor.LOAD_STATUS rightSiblingLoadStatus = targetsLoadStatus.get(rightSibling);
                            LatencyMonitor.LOAD_STATUS leftSiblingLoadStatus = targetsLoadStatus.get(leftSibling);
                            if (leftSiblingLoadStatus.compareTo(rightSiblingLoadStatus) < 0) // Left is better
                                leftOrRight = -1; // Select left
                            else if (leftSiblingLoadStatus.compareTo(rightSiblingLoadStatus) > 0) // Right is better
                                leftOrRight = 1; // Select right
                            else // Both are equal
                                leftOrRight = (Math.random() > 0.5) ? 1 : -1; // Randomly select
                        }
                    }
                    if (leftOrRight != 0) {
                        needUpdate = needUpdate || doMerge(target, leftOrRight);
                        // Set done for siblings
                        if (leftOrRight < 0) doneTargets.add(leftSibling);
                        else doneTargets.add(rightSibling);
                    }
                }
                doneTargets.add(target);
            }
        }
        if (needUpdate) // Avoid unnecessary update
            updateUpstreamRouter();
        LatencyMonitor.done(latencyMonitor, getContext()); // Tell latency monitor that load status has been processed.
    }

    private boolean doSplit(ActorRef target) { // Return whether splitting success or fail
        if (bolt.getMaxParallelism() <= subOperatorsInfo.size()) {
            logger.debug("Reach max parallelism, will not split");
            return false;
        }
        logger.info("Start splitting");
        // 1. Create new sub operator.
        // 2. Update upstream router.
        SubOperatorInfo targetInfo = subOperatorsInfo.get(target);
        int targetLowerBound = targetInfo.lowerBound;

        List<Address> nodes = resourceManager.allocateResource(1);
        if (nodes.isEmpty()) { // There is no more resources, or meet an error during resource allocating
            logger.warn("Allocating resource error when elastic scaling.");
            return false;
        }
        assert nodes.size() == 1;
        Address node = nodes.get(0);
        ActorRef newSubOperator = getContext().actorOf(BoltSubOperator.props(bolt, cloneTargetRouters())
                .withDeploy(new Deploy(new RemoteScope(node))));
        Future<Object> newPortFuture = Patterns.ask(newSubOperator, BoltSubOperator.PORT_PLEASE, Constants.futureTimeout);
        ActorRef newPort;
        try {
            newPort = (ActorRef) Await.result(newPortFuture, Constants.futureTimeout.duration());
        } catch (Exception e) {
            e.printStackTrace(System.err);
            logger.error("Future timeout when request sub operator port");
            return false;
        }

        int newLowerBound = router.split(targetLowerBound, newPort);
        subOperatorsInfo.put(newSubOperator, new SubOperatorInfo(newLowerBound, node, newPort));

        LatencyMonitor.addTarget(latencyMonitor, newSubOperator, newPort, getContext()); // Start monitoring new subOperator

        logger.info("Sub operator is split");
        return true;
    }

    private boolean doMerge(ActorRef target, int leftOrRight) { // Return whether merging success or fail
        if (bolt.getMinParallelism() >= subOperatorsInfo.size()) {
            logger.debug("Reach min parallelism, will not merge");
            return false;
        }
        logger.info("Start merging");
        SubOperatorInfo targetInfo = subOperatorsInfo.get(target);
        int targetLowerBound = targetInfo.lowerBound;
        if (leftOrRight < 0) {
            assert (router.leftSibling(targetLowerBound) != null);
            ActorRef leftSibling = portToTarget(router.leftSibling(targetLowerBound));
            SubOperatorInfo leftSiblingSubOperatorInfo = subOperatorsInfo.get(leftSibling);
            leftSiblingSubOperatorInfo.lowerBound = router.mergeLeft(targetLowerBound);
        } else {
            assert (router.rightSibling(targetLowerBound) != null);
            ActorRef rightSibling = portToTarget(router.rightSibling(targetLowerBound));
            SubOperatorInfo rightSiblingSubOperatorInfo = subOperatorsInfo.get(rightSibling);
            rightSiblingSubOperatorInfo.lowerBound = router.mergeRight(targetLowerBound);
        }

        LatencyMonitor.removeTarget(latencyMonitor, target, getContext()); // Remove merged sub-operator
        subOperatorsInfo.remove(target);
        // TODO: Stop useless sub operator after timeout, implement this after testing

        logger.info("Sub operator is merged");
        return true;
    }

    private boolean canBeMerged(Map<ActorRef, LatencyMonitor.LOAD_STATUS> targetsLoadStatus,
                                Set<ActorRef> doneTargets,
                                ActorRef sibling) {
        if (sibling != null) {
            LatencyMonitor.LOAD_STATUS loadStatus = targetsLoadStatus.get(sibling);
            return !doneTargets.contains(sibling) && (loadStatus == LatencyMonitor.LOAD_STATUS.UNDERLOAD
                    || loadStatus == LatencyMonitor.LOAD_STATUS.LOWLOAD
                    || loadStatus == LatencyMonitor.LOAD_STATUS.NORMORLLOAD);
        }
        return false;
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

    private ActorRef portToTarget(ActorRef port) {
        for (Entry<ActorRef, SubOperatorInfo> subOperatorInfoEntry : subOperatorsInfo.entrySet()) {
            // TODO Just traverse current now, if this becomes a bottleneck then optimize it
            if (subOperatorInfoEntry.getValue().port.equals(port))
                return subOperatorInfoEntry.getKey();
        }
        assert (true); // Should never be here
        return null;
    }

    private void doTest(Test test) {
        switch (test) {
            case SPLIT: { // Test split, split the first subOperator
                assert !subOperatorsInfo.isEmpty();
                Iterator<ActorRef> subOperatorIterator = subOperatorsInfo.keySet().iterator();
                ActorRef subOperator = subOperatorIterator.next();
                doSplit(subOperator);
                updateUpstreamRouter();
                break;
            }
            case MERGE: {
                assert !subOperatorsInfo.isEmpty();
                Iterator<ActorRef> subOperatorIterator = subOperatorsInfo.keySet().iterator();
                subOperatorIterator.next();
                ActorRef subOperator = subOperatorIterator.next();
                doMerge(subOperator, -1);
                updateUpstreamRouter();
                break;
            }
            default: break;
        }
    }
}
