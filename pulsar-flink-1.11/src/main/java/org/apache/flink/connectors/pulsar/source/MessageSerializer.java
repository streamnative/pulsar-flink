/**
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

package org.apache.flink.connectors.pulsar.source;

import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.io.Serializable;

/**
 * An interface for the deserialization of Pulsar messages.
 */
public interface MessageSerializer<T> extends Serializable {
    /**
     * Serializes the incoming element to a specified type.
     *
     * @param element The incoming element to be serialized
     * @return The serialized element.
     */
    void serialize(T element, TypedMessageBuilder<byte[]> messageBuilder);

    /**
     * Initialization method for the schema. It is called before the actual working methods
     * {@link #serialize(Object, TypedMessageBuilder)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access additional features such as e.g.
     * registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {
    }

    /**
     * Wraps a Flink {@link SerializationSchema} to a {@link MessageSerializer}.
     *
     * @param valueSerializer the serializer class used to serialize the value.
     * @param <V>             the value type.
     * @return A {@link MessageSerializer} that deserialize the value with the given deserializer.
     */
    static <V> MessageSerializer<V> valueOnly(SerializationSchema<V> valueSerializer) {
        return new MessageSerializer<V>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                valueSerializer.open(context);
            }

            @Override
            public void serialize(V element, TypedMessageBuilder<byte[]> messageBuilder) {
                messageBuilder.value(valueSerializer.serialize(element));
            }
        };
    }
}
