package com.pku.sault.engine.framework;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pku.sault.api.Task;
import com.pku.sault.api.Config;
import com.pku.sault.engine.cluster.ResourceManager;
import com.pku.sault.engine.operator.Operator;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.remote.RemoteScope;

public class Driver extends UntypedActor {
	public static class Edge implements Serializable {
		private static final long serialVersionUID = 1L;
		final String sourceId;
		final String targetId;
		final boolean toAdd;
		public Edge(String sourceId, String targetId) {
			this(sourceId, targetId, true);
		}
		public Edge(String sourceId, String targetId, boolean toAdd) {
			this.sourceId = sourceId;
			this.targetId = targetId;
			this.toAdd = toAdd;
		}
	}
	
	public static class Node implements Serializable {
		private static final long serialVersionUID = 1L;
		final String Id;
		final Task task;
		final boolean toAdd;
		public Node(String Id, Task task) {
			this(Id, task, true);
		}
		public Node(String Id, Task task, boolean toAdd) {
			this.Id = Id;
			this.task = task;
			this.toAdd = toAdd;
		}
	}
	
	private final Config config; // TODO Pass this to operator later
	private ResourceManager resourceManager;
	private Map<String, ActorRef> operators;
	
	Driver(Config config) {
		this.config = config;
		this.operators = new HashMap<String, ActorRef>();
		this.resourceManager = new ResourceManager(this.config, getContext());
	}
	
	public static Props props(final Config config) {
		return Props.create(new Creator<Driver>() {
			private static final long serialVersionUID = 1L;
			public Driver create() throws Exception {
				return new Driver(config);
			}
		});
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof Node) {
			Node node = (Node)msg;
			if (node.toAdd) { // Add operator
				if (!operators.containsKey(node.Id)) {
					// [Caution] Block here!
					List<Address> resource = resourceManager.allocateResource(1);
					assert(!resource.isEmpty()); // There should be extra resource
					ActorRef newOperator = getContext().actorOf(Operator.props(node.Id, node.task, resourceManager)
							.withDeploy(new Deploy(new RemoteScope(resource.get(0)))));
					operators.put(node.Id, newOperator);
				} else
					System.err.println("The node: " + node.Id + " already exsits.");
			} else { // Remove operator
				// TODO Can't directly kill, deal with this later
			}
		} else if (msg instanceof Edge) {
			Edge edge = (Edge)msg;
			if (edge.toAdd) { // Add edge
				if (operators.containsKey(edge.sourceId) && operators.containsKey(edge.targetId)) {
					ActorRef source = operators.get(edge.sourceId);
					ActorRef target = operators.get(edge.targetId);
					Operator.addTarget(edge.targetId, target, source, getContext());
				} else
					System.err.println("The edge: " + edge.sourceId + "-" + edge.targetId + " already exsits.");
			} else { // Remove edge
				// TODO Can't directly remove, deal with this later
			}
		} else unhandled(msg);
	}
}
