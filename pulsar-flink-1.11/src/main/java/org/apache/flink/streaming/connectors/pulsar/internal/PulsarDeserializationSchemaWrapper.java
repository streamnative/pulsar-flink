package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.pulsar.client.api.Message;

import java.io.IOException;

/**
 * A simple wrapper for using the DeserializationSchema with the PulsarDeserializationSchema
 * interface.
 * @param <T> The type created by the deserialization schema.
 */
@Internal
public class PulsarDeserializationSchemaWrapper<T> implements PulsarDeserializationSchema<T> {
    private static final long serialVersionUID = 2651665280744549932L;

    private final DeserializationSchema<T> deserializationSchema;

    public PulsarDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.deserializationSchema.open(context);
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
