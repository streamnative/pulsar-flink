package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;

/**
 * A wrapper for using the default DeserializationSchema with PulsarDeserializationSchemaWrapper.
 *
 * @param <T> The type created by the deserialization schema.
 */
public class PulsarDeserializationSchemaWrapper<T> implements PulsarDeserializationSchema<T> {

    private final DeserializationSchema<T> deserializationSchema;

    public PulsarDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema){
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return deserializationSchema.isEndOfStream(nextElement);
    }

    @Override
    public T deserialize(Message message) throws IOException {
        return deserializationSchema.deserialize(message.getData());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
