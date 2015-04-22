package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.pku.sault.api.Spout;
import com.pku.sault.engine.cluster.ResourceManager;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.remote.RemoteScope;

/**
 * Modified from Operator.
 * @author taotaotheripper
 */
public class SpoutOperator extends UntypedActor {

    // Activate or deactivate spout
    public static class Activate implements Serializable {
        private static final long serialVersionUID = 1L;
        final boolean toActivate;
        public Activate(boolean toActivate) {
            this.toActivate = toActivate;
        }
    }

    private Spout spout;
    private final String id;
    private Map<String, ActorRef> targets;
    private Map<String, RouteTree> targetRouters;

    private List<ActorRef> subOperators;

    private final ResourceManager resourceManager;
    private List<Address> resources;

    public static Props props(final String id, final Spout spout, final ResourceManager resourceManager) {
        // Pass empty targets in constructor
        return props(id, spout, resourceManager, null);
    }

    public static Props props(final String id, final Spout spout, final ResourceManager resourceManager,
                              final Map<String, ActorRef> targets) {
        return Props.create(new Creator<SpoutOperator>() {
            private static final long serialVersionUID = 1L;
            public SpoutOperator create() throws Exception {
                return new SpoutOperator(id, spout, resourceManager, targets);
            }
        });
    }

    SpoutOperator(String id, Spout spout, ResourceManager resourceManager, Map<String, ActorRef> targets) {
        this.id = id;
        this.spout = spout;
        this.resourceManager = resourceManager;
        this.targets = new HashMap<String, ActorRef>();
        this.targetRouters = new HashMap<String, RouteTree>();
        this.subOperators = new LinkedList<ActorRef>();

        // Request initial resource
        // [Caution] Block here!
        this.resources = this.resourceManager.allocateResource(spout.getParallelism());
        assert(!this.resources.isEmpty()); // There must be resource to start the operator
        // Start workers
        for (Address resource : this.resources) {
            // Start subOperator remotely
            ActorRef subOperator = getContext().actorOf(SpoutSubOperator.props(spout, targetRouters)
                    .withDeploy(new Deploy(new RemoteScope(resource))));
            subOperators.add(subOperator);
        }

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
        // Spout is the source operator, so there is no RouterRequest.
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
        } else if (msg instanceof Operator.Router) {
            // Update local target info
            Operator.Router targetRouter = (Operator.Router)msg;
            assert(targets.containsKey(targetRouter.OperatorID)); // target should have been inserted
            targetRouters.put(targetRouter.OperatorID, targetRouter.router);
            // Broadcast new router to all sub operators
            for (ActorRef subOperator : subOperators)
                subOperator.forward(msg, getContext());
        } else if (msg instanceof Activate) { // Activate or deactivate spout
            for (ActorRef subOperator : subOperators)
                subOperator.forward(msg, getContext());
        } else unhandled(msg);
        // TODO Dynamic merging and divide sub-operators
    }
}
