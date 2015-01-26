package com.pku.sault.engine.cluster;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.pku.sault.api.Config;

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
public class ResourceManager implements Serializable {
	private static final long serialVersionUID = 1L;
	
	static private long FUTURE_TIMEOUT = 120L; // 120 seconds
	static private Timeout futureTimeout = new Timeout(Duration.create(FUTURE_TIMEOUT, "seconds"));
	
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
    }
    
    /**
     * If there is no more resources, just return empty list.
     * @param number
     * @return
     */
    public List<Address> allocateResource(int number) {
    	Future<Object> resourcesFuture = Patterns.ask(resourceManagerActor, new AllocateResource(number), futureTimeout);
    	List<Address> resources;
    	try {
			resources = ((Resources)Await.result(resourcesFuture, futureTimeout.duration())).resources;
    	} catch (Exception e) {
    		System.err.println(e);
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
    	Future<Object> resourcesFuture = Patterns.ask(resourceManagerActor, new AllocateResource(number), futureTimeout);
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
}
