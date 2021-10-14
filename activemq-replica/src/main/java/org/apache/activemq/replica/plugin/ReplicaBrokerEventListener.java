package org.apache.activemq.replica.plugin;

import org.apache.activemq.command.ActiveMQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.IOException;

public class ReplicaBrokerEventListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final ReplicaEventProcessor eventProcessor = new ReplicaEventProcessor();

    @Override
    public void onMessage(final Message jmsMessage) {
        logger.trace("Received replication message from replica source");
        var message = (ActiveMQMessage) jmsMessage;

        try {
            logger.info(eventSerializer.deserializeMessageData(message.getContent()));
        } catch (IOException e) {
            logger.error("Failed to deserialize " + new String(message.getContent().data));
        }
    }
}
