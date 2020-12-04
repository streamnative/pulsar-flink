package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.io.Serializable;

/**
 * A {@link PulsarSerializationSchema} defines how to serialize values of type T into byte[].
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
