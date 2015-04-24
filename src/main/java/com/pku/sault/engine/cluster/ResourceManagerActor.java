package com.pku.sault.engine.cluster;

import java.util.List;

import akka.actor.Address;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

import com.pku.sault.api.Config;

class ResourceManagerActor extends UntypedActor {

    private ResourceFactory resourceFactory;

    private final String akkaSystemConfig = ""
            + "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
            + "akka.remote.netty.tcp.port = 0\n"
			+ "akka.remote.netty.tcp.tcp-nodelay = off\n"
			// Independent timer dispatcher
			+ "sault.timer-dispatcher {\n"
			+ "	type = Dispatcher\n"
			+ "	executor = \"fork-join-executor\"\n"
			+ " fork-join-executor.parallelism-min = 2\n"
			+ " fork-join-executor.parallelism-max = 2\n"
			+ "}\n"
			+ "akka.remote.netty.tcp.maximum-frame-size = 64000000b\n"
			// + "akka.stdout-loglevel = \"DEBUG\"\n"
			// + "akka.loglevel = \"DEBUG\"\n"
			// In fact, this is not really a solution
			// This can be only temp used, a better way may be use zeromq instead.
			// + "akka.remote.netty.tcp.send-buffer-size = 128000000b\n"
			// + "akka.remote.netty.tcp.receive-buffer-size = 128000000b\n"
			// + "akka.remote.netty.tcp.write-buffer-high-water-mark = 100000000b\n"
			// + "akka.remote.netty.tcp.write-buffer-low-water-mark = 64000000b\n"
			// + "akka.actor.remote.transport-failure-detector.acceptable-heartbeat-pause = 1000s\n"
			// + "akka.actor.remote.watch-failure-detector.acceptable-heartbeat-pause = 1000s\n"
			// + "akka.actor.default-remote-dispatcher.throughput = 100000"
			// + "akka.actor.remote.use-passive-connections = off"
			// + "akka.actor.default-dispatcher.fork-join-executor.parallelism-min = 64"
			// + "akka.actor.remote.default-remote-dispatcher.fork-join-executor.parallelism-min = 2\n"
			// + "akka.actor.remote.default-remote-dispatcher.fork-join-executor.parallelism-max = 2\n"
			// + "akka.actor.remote.system-message-buffer-size = "
			+ "akka.actor.remote.netty.tcp.client-socket-worker-pool.pool-size-min = 4\n"
			+ "akka.actor.remote.netty.tcp.client-socket-worker-pool.pool-size-max = 4\n"
			// + "akka.actor.remote.netty.tcp.server-socket-worker-pool.pool-size-min = 8\n"
			// + "akka.actor.remote.netty.tcp.server-socket-worker-pool.pool-size-max = 8\n"
			;

    ResourceManagerActor(Config config) {
    	this.resourceFactory = new SparkResourceFactory();
        this.resourceFactory.init(config, akkaSystemConfig);
    }
    
	static Props props(final Config config) {
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

    @Override
    public void postStop() {
        resourceFactory.close();
    }
}
