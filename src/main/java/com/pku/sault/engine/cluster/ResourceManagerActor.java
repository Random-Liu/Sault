package com.pku.sault.engine.cluster;

import java.util.List;

import akka.actor.Address;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

import com.pku.sault.util.SaultConfig;

class ResourceManagerActor extends UntypedActor {

    private SparkResourceFactory resourceFactory;
	
    ResourceManagerActor(SaultConfig config) {
    	this.resourceFactory = new SparkResourceFactory(config);
    }
    
	static Props props(final SaultConfig config) {
		return Props.create(new Creator<ResourceManagerActor>() {
			private static final long serialVersionUID = 1L;
			public ResourceManagerActor create() throws Exception {
				return new ResourceManagerActor(config);
			}
		});
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof ResourceManager.AllocateResource) {
			ResourceManager.AllocateResource resourceRequest = (ResourceManager.AllocateResource)msg;
			List<Address> resources = resourceFactory.allocateResources(resourceRequest.number);
			getSender().tell(new ResourceManager.Resources(resources), getSelf()); // Send resources back
		} else if (msg instanceof ResourceManager.ReleaseResource) {
			ResourceManager.ReleaseResource releaseRequest = (ResourceManager.ReleaseResource)msg;
			resourceFactory.releaseResources(releaseRequest.resources);
			// Asynchronous call, so there is no reply here
		} else unhandled(msg);
	}
}
