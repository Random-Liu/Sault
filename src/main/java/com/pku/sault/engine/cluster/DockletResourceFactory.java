package com.pku.sault.engine.cluster;

import akka.actor.Address;
import com.pku.sault.api.Config;

import java.util.List;

/**
 * Created by lantaoli on 2015/3/29.
 */
public class DockletResourceFactory implements ResourceFactory {

    @Override
    public void init(Config saultConfig, String actorSystemConfig) {

    }

    @Override
    public List<Address> allocateResources(int number) {
        return null;
    }

    @Override
    public void releaseResources(List<Address> resources) {

    }

    @Override
    public void close() {

    }
}
