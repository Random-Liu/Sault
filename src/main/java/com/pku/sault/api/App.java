package com.pku.sault.api;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import com.pku.sault.engine.framework.Driver;
import com.typesafe.config.ConfigFactory;

/*
class DirectGraph {
	private class Node {
		String Id;
		Bolt
	}
}
*/

public class App {
	private final static String DRIVER_SYSTEM_NAME = "SaultDriver";
	private final static com.typesafe.config.Config systemConfig =
			ConfigFactory.parseString(""
			+ "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
			+ "akka.remote.netty.tcp.port = 0");
	
	private Config config;
	private ActorSystem system;
	private ActorRef driver;
	
	public App(Config config) {
		this.config = config;
		this.system = ActorSystem.create(DRIVER_SYSTEM_NAME, systemConfig);
		this.driver = system.actorOf(Driver.props(this.config));
	}

	// TODO Create node graph, check placement of spouts and bolts.
	public boolean addNode(String id, Bolt bolt) {
		// Should be no sender
		driver.tell(new Driver.BoltNode(id, bolt), null);
		return true;
	}

	public boolean addNode(String id, Spout spout) {
		// Should be no sender
		driver.tell(new Driver.SpoutNode(id, spout), null);
		return true;
	}
	
	public boolean addEdge(String sourceId, String targetId) {
		// Should be no sender
		driver.tell(new Driver.Edge(sourceId, targetId), null);
		return true;
	}
	
	// TODO Add remove later
	// public boolean removeNode(String id)
	
	// TODO Add remove edge later
	// public boolean removeEdge(String sourceId, String targetId);
}
