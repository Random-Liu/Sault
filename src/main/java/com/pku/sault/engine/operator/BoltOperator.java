package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.*;
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
        SPLIT
        // Add more later
    }

    static class Split implements Serializable {
        private static final long serialVersionUID = 1L;
        final ActorRef subOperator;
        Split(ActorRef subOperator) {
            this.subOperator = subOperator;
        }
    }

    private class SubOperatorInfo {
        int lowerBound;
        ActorRef port;
        SubOperatorInfo(int lowerBound, ActorRef port) {
            this.lowerBound = lowerBound;
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
			ActorRef subOperator = getContext().actorOf(BoltSubOperator.props(this.bolt, targetRouters)
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
            int lowerBound = lowerBounds.get(subOperatorIndex);
            int upperBound = router.getUpperBound(lowerBound);
            ActorRef subOperator = subOperators.get(subOperatorIndex);
            portFutures.add(Patterns.ask(subOperator,
                    new BoltSubOperator.InitPort(lowerBound, upperBound), Constants.futureTimeout));
        }
		Future<Iterable<Object>> portsFuture = sequence(portFutures, getContext().dispatcher());
		try {
			Iterable<Object> portObjects = Await.result(portsFuture, Constants.futureTimeout.duration());
            int portIndex = 0;
			for (Object portObject : portObjects) {
                ActorRef port = (ActorRef) portObject;
                int lowerBound = lowerBounds.get(portIndex);
                subOperatorsInfo.put(subOperators.get(portIndex), new SubOperatorInfo(lowerBound,
                        port));
                router.setTarget(lowerBound, port); // Fill the empty cell
                ++portIndex;
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
		assert (router != null);

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
            // Number of sub operator should never exceed max parallelism.
            assert bolt.getMaxParallelism() > subOperatorsInfo.size();
            // 1. Create new sub operator (with original operator port, range)
            // 2. Update the input route table of the original sub operator (with new range).
            // After that, the original sub operator will redirect to the new sub operator all messages belong to it.
            // 3-a. Operator update all upper stream operator's route table.
            // 3-b. Sub operator starts new worker when message comes. It starts the new worker with port of original
            // sub operator and the key it should process. After the worker is started, it will send an command message
            // with the key to get the state from the original actor. During that, the worker will pending all the incoming
            // messages in a buffer. After the state is set, it will process all the messages pending before.
            // After all these 3 steps, the elastic scale up is done.
            Split split = (Split)msg;
            ActorRef originalSubOperator = split.subOperator;
            SubOperatorInfo originalSubOperatorInfo = subOperatorsInfo.get(originalSubOperator);
            ActorRef originalPort = originalSubOperatorInfo.port;
            int originalLowerBound = originalSubOperatorInfo.lowerBound;

            List<Address> nodes = resourceManager.allocateResource(1);
            if (nodes.isEmpty()) { // There is no more resources, or meet an error during resource allocating
                logger.warn("Allocating resource error when elastic scaling.");
                return;
            }
            assert nodes.size() == 1;
            Address node = nodes.get(0);
            ActorRef newSubOperator = getContext().actorOf(BoltSubOperator.props(bolt, targetRouters)
                    .withDeploy(new Deploy(new RemoteScope(node))));
            int newLowerBound = router.split(originalLowerBound, null); // Fill this cell later
            int upperBound = router.getUpperBound(newLowerBound);
            Future<Object> newPortFuture = Patterns.ask(newSubOperator,
                    new BoltSubOperator.InitPort(newLowerBound, upperBound, originalPort), Constants.futureTimeout);
            ActorRef newPort = (ActorRef)Await.result(newPortFuture, Constants.futureTimeout.duration());
            router.setTarget(newLowerBound, newPort);
            subOperatorsInfo.put(newSubOperator, new SubOperatorInfo(newLowerBound, newPort));

            for (ActorRef source : sources.values())
                source.tell(new Operator.Router(id, router), getSelf()); // Update router of all sources
        } else if (msg instanceof Test) {
            doTest((Test) msg); // Only used for test
        } else unhandled(msg);
		// TODO Dynamic merging and divide sub-operators
	}

    private void doTest(Test test) {
        switch (test) {
            case SPLIT: // Test split, split the first subOperator
                assert !subOperatorsInfo.isEmpty();
                Iterator<ActorRef> subOperatorIterator = subOperatorsInfo.keySet().iterator();
                ActorRef subOperator = subOperatorIterator.next();
                getSelf().tell(new Split(subOperator), getSelf());
                break;
            default: break;
        }
    }
}
