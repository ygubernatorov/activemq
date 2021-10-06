package org.apache.activemq.replica.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;

public class ReplicaBroker extends BrokerFilter {

    public ReplicaBroker(final Broker next) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
    }
}
