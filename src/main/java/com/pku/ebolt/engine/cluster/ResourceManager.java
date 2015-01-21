package com.pku.ebolt.engine.cluster;

import java.io.Serializable;
import java.util.List;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.util.Timeout;

// TODO Consider class loading problem in the cluster!
public class ResourceManager extends UntypedActor {

	static private long FUTRUE_TIMEOUT = 120L; // 120 seconds
	static private Timeout futrueTimeout = new Timeout(Duration.create(FUTRUE_TIMEOUT, "seconds"));
	
    public static class ResourceRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public final int number;
        public ResourceRequest(int number) {
        	this.number = number;
        }
    }

    public static class Resources implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public final List<String> nodes;
        Resources(List<String> nodes) {
        	this.nodes = nodes;
        }
    }
	
    static public Resources requestResource(int number, ActorRef resourceManager, ActorRef self) throws Exception {
    	Future<Object> resourceFutrue = Patterns.ask(resourceManager, new ResourceRequest(number), futrueTimeout);
    	return (Resources)Await.result(resourceFutrue, futrueTimeout.duration());
    }
    
	@Override
	public void onReceive(Object msg) throws Exception {
		unhandled(msg);
	}
}
