package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the Pulsar reader messages
 * into data types (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
public interface PulsarDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Method to decide whether the element signals the end of the stream. If
     * true is returned the element will not be emitted.
     *
     * @param nextElement The element to check for the end-of-stream signal.
     * @return True, if the element signals end of stream, false otherwise.
     */
    boolean isEndOfStream(T nextElement);

    /**
     * Deserializes the Pulsar message.
     *
     * @param message Pulsar message to be deserialized.
     * @return The deserialized message as an object.
     * @throws IOException
     */
    T deserialize(Message message) throws IOException;
}
