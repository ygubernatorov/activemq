package org.apache.activemq.replica.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageAck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final ReplicaBrokerSubscriptionHandler subscriptionHandler;
    private final Broker broker;

    ReplicaBrokerEventListener(final Broker broker) {
        this.broker = requireNonNull(broker);
        this.subscriptionHandler = new ReplicaBrokerSubscriptionHandler(broker);
    }

    @Override
    public void onMessage(final Message jmsMessage) {
        logger.trace("Received replication message from replica source");
        var message = (ActiveMQMessage) jmsMessage;
        var messageContent = message.getContent();

        try {
            var deserializedData = eventSerializer.deserializeMessageData(messageContent);
            logger.trace(deserializedData.toString());
            getEventType(message).ifPresent(eventType -> {
                switch (eventType) {
                    case MESSAGE_SEND:
                        logger.trace("Processing replicated message send");
                        persistMessage((ActiveMQMessage) deserializedData);
                        return;
                    case MESSAGE_ACK:
                        logger.trace("Processing replicated message ack");
                        consumeAck((MessageAck) deserializedData);
                        return;
                    case MESSAGE_CONSUMED:
                        logger.trace("Processing replicated consume");
                        consumeMessage((MessageReference) deserializedData);
                        return;
                    case DESTINATION_UPSERT:
                        logger.trace("Processing replicated destination");
                        upsertDestination((ActiveMQDestination) deserializedData);
                        return;
                    case DESTINATION_DELETE:
                        logger.trace("Processing replicated destination deletion");
                        deleteDestination((ActiveMQDestination) deserializedData);
                        return;
                    default:
                        logger.warn("Unhandled event type \"{}\" for replication message id: {}", eventType, message.getJMSMessageID());
                }
            });
        } catch (IOException | ClassCastException e) {
            logger.error("Failed to deserialize replication message (id={}), {}", message.getMessageId(), new String(messageContent.data));
            logger.debug("Deserialization error for replication message (id={})", message.getMessageId(), e);
        }
    }

    private void persistMessage(final ActiveMQMessage message) {
        try {
            new ReplicaInternalMessageProducer(broker).produceToReplicaQueue(message);
        } catch (Exception e) {
            logger.error("Failed to process message {} with JMS message id: {}", message.getMessageId(), message.getJMSMessageID(), e);
        }
    }

    private void consumeAck(final MessageAck ack) {
        try {
            var consumerBrokerExchange = new ConsumerBrokerExchange();
            var destination = broker.getDestinations(ack.getDestination()).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Destination not found that matches: " + ack.getDestination().getQualifiedName()));
            consumerBrokerExchange.setRegion(broker);
            consumerBrokerExchange.setRegionDestination(destination);
            consumerBrokerExchange.setConnectionContext(broker.getAdminConnectionContext());
            subscriptionHandler.createSubscriptionIfAbsent(ack.getConsumerId(), ack.getDestination());
            var regionBroker = (RegionBroker) broker.getAdaptor(RegionBroker.class);
            var region = regionBroker.getRegion(destination.getActiveMQDestination());
            region.acknowledge(consumerBrokerExchange, ack);
        } catch (Exception e) {
            logger.error("Failed to process ack with last message id: {}", ack.getLastMessageId(), e);
        }
    }

    private Optional<ReplicaEventType> getEventType(final ActiveMQMessage message) {
        try {
            var eventTypeProperty = message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY);
            return Arrays.stream(ReplicaEventType.values())
                .filter(t -> t.name().equals(eventTypeProperty))
                .findFirst();
        }
        catch (JMSException e) {
            logger.error("Failed to get {} property {}", ReplicaEventType.class.getSimpleName(), ReplicaEventType.EVENT_TYPE_PROPERTY, e);
            return Optional.empty();
        }
    }

    private void consumeMessage(final MessageReference messageReference) {
        broker.getRoot().messageConsumed(broker.getAdminConnectionContext(), messageReference);
    }

    private void upsertDestination(final ActiveMQDestination destination) {
        try {
            var isExistingDestination = Arrays.stream(broker.getDestinations())
                .anyMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isExistingDestination) {
                logger.debug("Destination [{}] already exists, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
        }
        try {
            broker.addDestination(broker.getAdminConnectionContext(), destination, true);
        } catch (Exception e) {
            logger.error("Unable to add destination [{}]", destination, e);
        }
    }

    private void deleteDestination(final ActiveMQDestination destination) {
        try {
            var isNonExtantDestination = Arrays.stream(broker.getDestinations())
                .noneMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isNonExtantDestination) {
                logger.debug("Destination [{}] does not exist, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
        }
        try {
            broker.removeDestination(broker.getAdminConnectionContext(), destination, 1000);
        } catch (Exception e) {
            logger.error("Unable to remove destination [{}]", destination, e);
        }
    }

}
