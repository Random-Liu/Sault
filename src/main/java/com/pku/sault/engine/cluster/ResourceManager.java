package com.pku.sault.engine.cluster;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import akka.actor.ActorSystem;
import com.pku.sault.api.Config;

import com.pku.sault.engine.util.Constants;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.pattern.Patterns;
import akka.util.Timeout;

/**
 * Resource manager interface. This interface can be serialized
 * across the cluster.
 * TODO How to deal with code deploy?
 * @author taotaotheripper
 *
 */
// TODO Modify to new version of resource management
public class ResourceManager implements Serializable {
	private static final long serialVersionUID = 1L;

    private HashMap<Address, ActorSystem> localResources;

    // TODO Configuable later
    private final String akkaSystemConfig = ""
            + "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
            + "akka.remote.netty.tcp.port = 0\n"
            // + "akka.remote.netty.tcp.tcp-nodelay = off\n"
            + "akka.actor.remote.transport-failure-detector.acceptable-heartbeat-pause = 1000s\n"
            + "akka.actor.remote.watch-failure-detector.acceptable-heartbeat-pause = 1000s\n";

    static class AllocateResource implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final int number;
        AllocateResource(int number) {
        	this.number = number;
        }
    }

    static class ReleaseResource implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final List<Address> resources;
        ReleaseResource(List<Address> resources) {
        	this.resources = resources;
        }
    }
    
    public static class Resources implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public final List<Address> resources;
        Resources(List<Address> resources) {
        	this.resources = resources;
        }
    }
    
    private final ActorRef resourceManagerActor;
	
    public ResourceManager(Config config, ActorContext context) {
    	this.resourceManagerActor = context.actorOf(ResourceManagerActor.props(config));
        this.localResources = new HashMap<Address, ActorSystem>();
    }
    
    /**
     * If there is no more resources, just return empty list.
     * @param number
     * @return
     */
    public List<Address> allocateResource(int number) {
    	Future<Object> resourcesFuture = Patterns.ask(resourceManagerActor, new AllocateResource(number), Constants.futureTimeout);
    	List<Address> resources;
    	try {
			resources = ((Resources)Await.result(resourcesFuture, Constants.futureTimeout.duration())).resources;
        } catch (Exception e) {
            e.printStackTrace();
    		resources = new LinkedList<Address>(); //Return an empty resource list
    	}
		return resources;
    }
    
    /**
     * Asynchronously allocate resources. Allocated resources will be sent back
     * in message ResourceManager.Resources
     * @param number
     * @param context
     */
    public void asyncAllocateResource(int number, ActorContext context) {
    	Future<Object> resourcesFuture = Patterns.ask(resourceManagerActor, new AllocateResource(number), Constants.futureTimeout);
    	Patterns.pipe(resourcesFuture, context.dispatcher()).to(context.self());
    }
    
    /**
     * Asynchronously release resources.
     * @param resources
     * @param context
     */
    public void releaseReource(List<Address> resources, ActorContext context) {
    	resourceManagerActor.tell(new ReleaseResource(resources), context.self());
    }


    /**
     * Allocate local resource.
     * @return Address of local resource.
     */
    public Address allocateLocalResource(String resourceId) {
        ActorSystem localResource = ActorSystem.create(resourceId, ConfigFactory.parseString(akkaSystemConfig));
        Address localResourceAddress = localResource.provider().getDefaultAddress();
        localResources.put(localResourceAddress, localResource);
        return localResourceAddress;
    }

    /**
     * Release local resource.
     * @param resourceAddress Address of local resource.
     */
    public void releaseLocalResource(Address resourceAddress) {
        ActorSystem localResource = localResources.remove(resourceAddress);
        localResource.shutdown();
    }
}
