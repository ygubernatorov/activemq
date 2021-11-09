package org.apache.activemq.replica;

import org.apache.activemq.command.Message;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import java.io.IOException;

public class ReplicaEventSerializer {

    private final WireFormat wireFormat = new OpenWireFormat();

    byte[] serializeReplicationData(final Object object) throws IOException {
        try {
            ByteSequence packet = wireFormat.marshal(object);
            return ByteSequenceData.toByteArray(packet);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize data: " + object.toString() + " in container: " + e, e);
        }
    }

    byte[] serializeMessageData(final Message message) throws IOException {
        try {
            ByteSequence packet = wireFormat.marshal(message);
            return ByteSequenceData.toByteArray(packet);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize message: " + message.getMessageId() + " in container: " + e, e);
        }
    }

    Object deserializeMessageData(final ByteSequence sequence) throws IOException {
        return wireFormat.unmarshal(sequence);
    }
}
