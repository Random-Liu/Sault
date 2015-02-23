package com.pku.sault.api;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import com.pku.sault.engine.framework.Driver;
import com.typesafe.config.ConfigFactory;

import java.util.*;

public class App {
	class Graph {
		private class Node {
			Set<String> sources; // If sources is null, it means that this is a source only node
			Set<String> targets;
			Node(boolean isSource) {
				sources = null;
				if (!isSource) sources = new HashSet<String>();
				targets = new HashSet<String>();
			}
			boolean isSource() {
				return sources == null;
			}
		}
		Map<String, Node> graph = new HashMap<String, Node>();
		// All the APIs below return true when they really take effect, otherwise return false.
		boolean addSource(String id) {
			if (graph.containsKey(id)) return false;
			graph.put(id, new Node(true));
			return true;
		}
		boolean addNode(String id) {
			if (graph.containsKey(id)) return false;
			graph.put(id, new Node(false));
			return true;
		}
		boolean removeNode(String id) {
			Node node = graph.get(id);
			if (node == null) return false;
			for (String target : node.targets)
				graph.get(target).sources.remove(id);
			if (!node.isSource()) {
				for (String source : node.sources)
					graph.get(source).targets.remove(id);
			}
			graph.remove(id);
			return true;
		}
        boolean hasNode(String id) {
            Node node = graph.get(id);
            return node != null && !node.isSource();
        }
		boolean addEdge(String sourceId, String targetId) {
			Node source = graph.get(sourceId);
			Node target = graph.get(targetId);
			if (source == null || target == null) return false;
			if (target.isSource()) return false;
			if (source.targets.contains(targetId) || target.sources.contains(sourceId)) return false;
			source.targets.add(targetId);
			target.sources.add(sourceId);
			return true;
		}
		boolean removeEdge(String sourceId, String targetId) {
			Node source = graph.get(sourceId);
			Node target = graph.get(targetId);
			if (source == null || target == null) return false;
			if (target.isSource()) return false;
			if (!source.targets.contains(targetId) || !target.sources.contains(sourceId)) return false;
			source.targets.remove(targetId);
			target.sources.remove(sourceId);
			return true;
		}
	}

	private final static String DRIVER_SYSTEM_NAME = "SaultDriver";
	private final static com.typesafe.config.Config systemConfig =
			ConfigFactory.parseString(""
			+ "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
			+ "akka.remote.netty.tcp.port = 0");
	
	private Config config;
	private ActorSystem system;
	private ActorRef driver;
	private Graph graph;
	
	public App(Config config) {
		this.config = config;
		this.system = ActorSystem.create(DRIVER_SYSTEM_NAME, systemConfig);
		this.driver = system.actorOf(Driver.props(this.config));
		this.graph = new Graph();
	}

	public boolean addNode(String id, Bolt bolt) {
		return addNode(id, bolt, null);
	}

	public boolean addNode(String id, Spout spout) {
		return addNode(id, spout, null);
	}

	public boolean addNode(String id, Bolt bolt, List<String> targetIds) {
		if (!graph.addNode(id)) return false;
		if (targetIds != null) {
			for (String targetId : targetIds) {
				if (!graph.addEdge(id, targetId)) {
					graph.removeNode(id);
					return false;
				}
			}
		}
		driver.tell(new Driver.BoltNode(id, bolt, targetIds), ActorRef.noSender());
		return true;
	}

	public boolean addNode(String id, Spout spout, List<String> targetIds) {
		if (!graph.addSource(id)) return false;
		if (targetIds != null) {
			for (String targetId : targetIds) {
				if (!graph.addEdge(id, targetId)) {
					graph.removeNode(id);
					return false;
				}
			}
		}
		driver.tell(new Driver.SpoutNode(id, spout, targetIds), ActorRef.noSender());
		return true;
	}

	public boolean addEdge(String sourceId, String targetId) {
		if (!graph.addEdge(sourceId, targetId)) return false;
		driver.tell(new Driver.Edge(sourceId, targetId), ActorRef.noSender());
		return true;
	}

	// TODO Add remove later
	// public boolean removeNode(String id)
	
	// TODO Add remove edge later
	// public boolean removeEdge(String sourceId, String targetId);

    // Manually split bolt, this is only used in testing
    public boolean splitNode(String boltId) {
        if (!graph.hasNode(boltId)) return false;
        driver.tell(new Driver.Split(boltId), ActorRef.noSender());
        return true;
    }
}
