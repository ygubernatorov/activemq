package org.apache.activemq.replica.plugin;

import org.apache.activemq.util.ByteSequence;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import static java.util.Objects.requireNonNull;

public class ReplicaEvent {

    public interface WriteConsumer {
        void writeTo(ObjectOutput output) throws IOException;
    }

    private ReplicaEventType eventType;
    private byte[] eventData;

    ReplicaEvent setEventType(final ReplicaEventType eventType) {
        this.eventType = requireNonNull(eventType);
        return this;
    }

    ReplicaEvent setEventData(final byte[] eventData) {
        this.eventData = requireNonNull(eventData);
        return this;
    }

    ReplicaEvent acceptDataWrite(WriteConsumer writeConsumer) throws IOException {
        try (final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
             final ObjectOutput outputStream = new ObjectOutputStream(buffer)) {
            writeConsumer.writeTo(outputStream);
            this.eventData = buffer.toByteArray();
        }
        return this;
    }

    ByteSequence getEventData() {
        return new ByteSequence(eventData);
    }

    ReplicaEventType getEventType() {
        return eventType;
    }
}
