package org.apache.activemq.replica;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jms.JMSException;
import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class ReplicaBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBroker.class);
    private final ScheduledExecutorService brokerConnectionPoller = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean isConnecting = new AtomicBoolean();
    private final AtomicReference<ActiveMQConnection> connection = new AtomicReference<>();
    private final AtomicReference<ActiveMQSession> connectionSession = new AtomicReference<>();
    private final AtomicReference<ActiveMQMessageConsumer> eventConsumer = new AtomicReference<>();
    private final ActiveMQConnectionFactory replicaSourceConnectionFactory;

    public ReplicaBroker(final Broker next, final ActiveMQConnectionFactory replicaSourceConnectionFactory) {
        super(next);
        this.replicaSourceConnectionFactory = requireNonNull(replicaSourceConnectionFactory, "Need connection details of replica source for this broker");
        requireNonNull(replicaSourceConnectionFactory.getBrokerURL(), "Need connection URI of replica source for this broker");
    }

    @Override
    public void start() throws Exception {
        super.start();
        brokerConnectionPoller.scheduleAtFixedRate(this::beginReplicationIdempotent, 5, 5, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        ActiveMQMessageConsumer consumer = eventConsumer.get();
        ActiveMQSession session = connectionSession.get();
        ActiveMQConnection brokerConnection = connection.get();
        if (consumer != null) {
            consumer.stop();
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
        if (brokerConnection != null) {
            brokerConnection.close();
        }
        super.stop();
    }

    private void beginReplicationIdempotent() {
        if (connectionSession.get() == null) {
            logger.debug("Establishing inter-broker replication connection");
            establishConnectionSession();
        }
        if (eventConsumer.get() == null) {
            try {
                logger.debug("Creating replica event consumer");
                consumeReplicationEvents();
            } catch (Exception e) {
                logger.error("Could not establish replication consumer", e);
            }
        }
    }

    private void establishConnectionSession() {
        if (isConnecting.compareAndSet(false, true)) {
            logger.debug("Trying to connect to replica source");
            try {
                establishConnection();
                ActiveMQSession session = (ActiveMQSession) connection.get().createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
                session.setAsyncDispatch(false); // force the primary broker to block if we are slow
                connectionSession.set(session);
            } catch (RuntimeException | JMSException e) {
                logger.warn("Failed to establish connection to replica", e);
            } finally {
                if (connectionSession.get() == null) {
                    logger.info("Closing connection session after unsuccessful connection establishment");
                    connection.getAndUpdate(conn -> {
                        try {
                            if (conn != null) {
                                conn.close();
                            }
                        } catch (JMSException e) {
                            logger.error("Failed to close connection after session establishment failed", e);
                        }
                        return null;
                    });
                }
                isConnecting.weakCompareAndSetPlain(true, false);
            }
        }
    }

    private void establishConnection() throws JMSException {
        logger.trace("Replica connection URL {}", replicaSourceConnectionFactory.getBrokerURL());
        ActiveMQConnection newConnection = (ActiveMQConnection) replicaSourceConnectionFactory.createConnection();
        newConnection.start();
        connection.set(newConnection);
        logger.debug("Established connection to replica source: {}", replicaSourceConnectionFactory.getBrokerURL());
    }

    private void consumeReplicationEvents() throws JMSException {
        if (connectionUnusable() || sessionUnusable()) {
            return;
        }
        ActiveMQQueue replicationSourceQueue = connection.get()
            .getDestinationSource()
            .getQueues()
            .stream()
            .filter(d -> d.getPhysicalName().startsWith(ReplicaSupport.REPLICATION_QUEUE_PREFIX))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                MessageFormat.format("There is no replication queue on the source broker {0}", replicaSourceConnectionFactory.getBrokerURL())
            ));
        logger.debug("Plugin will mirror events from queue {}", replicationSourceQueue.getPhysicalName());
        eventConsumer.set((ActiveMQMessageConsumer)
            connectionSession.get().createConsumer(replicationSourceQueue, new ReplicaBrokerEventListener(getNext()))
        );
    }


    private boolean connectionUnusable() {
        if (isConnecting.get()) {
            logger.trace("Will not consume events because we are still connecting");
            return true;
        }
        ActiveMQConnection conn = connection.get();
        if (conn == null) {
            logger.trace("Will not consume events because we don't have a connection");
            return true;
        }
        if (conn.isClosed() || conn.isClosing()) {
            logger.trace("Will not consume events because the connection is not open");
            return true;
        }
        return false;
    }

    private boolean sessionUnusable() {
        ActiveMQSession session = connectionSession.get();
        if (session == null) {
            logger.trace("Will not consume events because we don't have a session");
            return true;
        }
        if (session.isClosed()) {
            logger.trace("Will not consume events because the session is not open");
            return true;
        }
        return false;
    }
}
