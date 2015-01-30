package com.pku.sault.engine.framework;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pku.sault.api.Bolt;
import com.pku.sault.api.Spout;
import com.pku.sault.api.Config;
import com.pku.sault.engine.cluster.ResourceManager;
import com.pku.sault.engine.operator.BoltOperator;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.remote.RemoteScope;
import com.pku.sault.engine.operator.Operator;
import com.pku.sault.engine.operator.SpoutOperator;

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

	private static class Node implements Serializable {
		private static final long serialVersionUID = 1L;
		final String Id;
		final boolean toAdd;
		Node(String Id, boolean toAdd) {
			this.Id = Id;
			this.toAdd = toAdd;
		}
	}

	public static class BoltNode extends Node {
		private static final long serialVersionUID = 1L;
		final Bolt bolt;
		public BoltNode(String Id, Bolt bolt) {
			super(Id, true);
			this.bolt = bolt;
		}
		public BoltNode(String Id, Bolt bolt, boolean toAdd) {
			super(Id, toAdd);
			this.bolt = bolt;
		}
	}

	public static class SpoutNode extends Node {
		private static final long serialVersionUID = 1L;
		final Spout spout;
		public SpoutNode(String Id, Spout spout) {
			super(Id, true);
			this.spout = spout;
		}
		public SpoutNode(String Id, Spout spout, boolean toAdd) {
			super(Id, toAdd);
			this.spout = spout;
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
					ActorRef newOperator;
					if (msg instanceof BoltNode) {
						newOperator = getContext().actorOf(BoltOperator.props(node.Id, ((BoltNode) msg).bolt,
								resourceManager).withDeploy(new Deploy(new RemoteScope(resource.get(0)))));
					} else {
						newOperator = getContext().actorOf(SpoutOperator.props(node.Id, ((SpoutNode) msg).spout,
								resourceManager).withDeploy(new Deploy(new RemoteScope(resource.get(0)))));
					}
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
