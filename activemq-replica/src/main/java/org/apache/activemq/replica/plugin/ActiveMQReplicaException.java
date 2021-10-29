package org.apache.activemq.replica.plugin;

public class ActiveMQReplicaException extends RuntimeException {

    public ActiveMQReplicaException(String message) {
        super(message);
    }

    public ActiveMQReplicaException(String message, Throwable cause) {
        super(message, cause);
    }
}
