package com.pku.sault.engine.cluster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import akka.actor.ActorSystem;
import akka.actor.Address;

import com.pku.sault.api.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Allocate and release resources on Spark cluster.
 * @author taotaotheripper
 *
 */
public class SparkResourceFactory {

	private JavaSparkContext context;
	// TODO Only use very simple data structure current now
	private List<Integer> resources;
	private final int resourceOfNode;
	private List<Address> nodes;
	private final int nodeNumber;
	
	private static class NodeStarter implements Function2<Integer, Iterator<Integer>, Iterator<Address>> {
		private static final long serialVersionUID = 1L;

		public Iterator<Address> call(Integer index, Iterator<Integer> dummyIter)
				throws Exception {
			final String ACTOR_SYSTEM_NAME = "SaultWorker";
			// TODO Set other configuration later
			// Use full name to avoid conflict with Config in API layer
			final com.typesafe.config.Config systemConfig = ConfigFactory.parseString(""
					+ "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
					+ "akka.remote.netty.tcp.port = 0");
			ActorSystem system = ActorSystem.create(ACTOR_SYSTEM_NAME+"-"+index, systemConfig);
			// TODO Start necessary daemon actors later
			LinkedList<Address> nodes = new LinkedList<Address>();
			nodes.add(system.provider().getDefaultAddress());
			return nodes.iterator();
		}
	}
	
	// TODO Use context.addJars to deploy application
	SparkResourceFactory(Config config) {
		this.resourceOfNode = config.getResourceOfNode();
		this.nodeNumber = config.getNodeNumber();
		
		SparkConf conf = new SparkConf().setAppName(config.getApplicationName())
				.setMaster(config.getSparkMaster());
		context = new JavaSparkContext(conf);
		
		List<Integer> dummy = new LinkedList<Integer>();
		JavaRDD<Integer> dummyRDD = context.parallelize(dummy, this.nodeNumber);
		this.nodes = dummyRDD.mapPartitionsWithIndex(new NodeStarter(), false).collect();
		
		// Use array list, because we usually need randomly access
		this.resources = new ArrayList<Integer>();
		for (int nodeId = 0; nodeId < nodeNumber; ++nodeId)
			this.resources.add(resourceOfNode);
	}
	
	/**
	 * Allocate one resource. If no more resources, return null.
	 * @return
	 */
	// If there is no more resources, return null
	Address allocateResource() {
		int bestNode = 0;
		for (int nodeId = 0; nodeId < nodeNumber; ++nodeId) {
			if (resources.get(bestNode) < resources.get(nodeId))
				bestNode = nodeId;
		}
		// If there is no more resources
		if (resources.get(bestNode) == 0) return null;
		resources.set(bestNode, resources.get(bestNode) - 1);
		return nodes.get(bestNode);
	}
	
	/**
	 * Allocate number resources. If no more resources, return empty list.
	 * @param number
	 * @return
	 */
	List<Address> allocateResources(int number) {
		List<Address> allocatedResources = new LinkedList<Address>();
		for (int i = 0; i < number; ++i) {
			Address resource = allocateResource();
			if (resource == null)
				break;
			allocatedResources.add(resource);
		}
		return allocatedResources;
	}

	/**
	 * Release one resource.
	 * @param resource
	 */
	void releaseResource(Address resource) {
		int nodeId;
		for (nodeId = 0; nodeId < nodeNumber; ++nodeId)
			if (nodes.get(nodeId).equals(resource)) break;
		resources.set(nodeId, resources.get(nodeId) + 1);
	}
	
	/**
	 * Release resources.
	 * @param resources
	 */
	void releaseResources(List<Address> resources) {
		for (Address resource : resources)
			releaseResource(resource);
	}
	
	/**
	 * Test main method
	 * @param args
	 */
	public static void main(String[] args) {
		Config config = new Config().setResourceOfNode(1).setNodeNumber(4);
		SparkResourceFactory resourceFactory = new SparkResourceFactory(config);
		// Test allocateResource
		for (int i = 0; i < 2; ++i)
			System.out.println(resourceFactory.allocateResource());
		// Test allocateResources
		resourceFactory.releaseResources(resourceFactory.allocateResources(4));
		System.out.println(resourceFactory.allocateResources(4));
	}
}
