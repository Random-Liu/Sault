package com.pku.sault.engine.cluster;

import akka.actor.Address;
import com.pku.sault.api.Config;

import java.util.List;

/**
 * Created by lantaoli on 2015/3/28.
 */
interface ResourceFactory {
    /**
     * Initialize the resource factory.
     * @param saultConfig Sault Configuration.
     * @param actorSystemConfig ActorSystem Configuration.
     */
    void init(Config saultConfig, String actorSystemConfig);

    /**
     * Allocate resources. If no more resources, return empty list.
     * @param number Number of resources needed.
     * @return Allocated resources.
     */
    List<Address> allocateResources(int number);

    /**
     * Release resources.
     * @param resources Resources need to be released.
     */
    void releaseResources(List<Address> resources);

    /**
     * Close the resource factory.
     */
    void close();
}
