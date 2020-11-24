package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.io.Serializable;

/**
 * A {@link PulsarSerializationSchema} defines how to serialize values of type {@code T} into {@link
 * byte[]}.
 *
 * <p>Please also implement {@link PulsarContextAware} if your serialization schema needs
 * information
 * about the available partitions and the number of parallel subtasks along with the subtask ID on
 * which the Pulsar Sink is running.
 *
 * @param <T> the type of values being serialized
 */
@PublicEvolving
public interface PulsarSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods
     * {@link #serialize(Object)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access additional
     * features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {
    }

    /**
     * Serializes given element and returns it as a byte[].
     *
     * @param element element to be serialized
     * @return byte[] represents the serialized data
     */
    byte[] serialize(T element);

    void serialize(T element, TypedMessageBuilder<byte[]> messageBuilder);

    Schema<?> getPulsarSchema();
}
