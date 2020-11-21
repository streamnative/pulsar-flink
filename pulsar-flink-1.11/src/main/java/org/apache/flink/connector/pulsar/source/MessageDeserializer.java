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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serializable;

/**
 * An interface for the deserialization of Pulsar messages.
 */
public interface MessageDeserializer<T> extends Serializable, ResultTypeQueryable<T> {
    /**
     * Initialization method for the schema. It is called before the actual working methods
     * {@link #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access additional features such as e.g.
     * registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {
    }
    /**
     * Deserialize a consumer record into the given collector.
     *
     * @param message the {@code Message} to deserialize.
     * @throws IOException if the deserialization failed.
     */
    void deserialize(Message<?> message, Collector<T> collector) throws IOException;

    /**
     * Method to decide whether the element signals the end of the stream. If
     * true is returned the element won't be emitted.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     * @return True, if the element signals end of stream, false otherwise.
     */
    boolean isEndOfStream(T nextElement);

    /**
     * Wraps a Flink {@link DeserializationSchema} to a {@link MessageDeserializer}.
     *
     * @param valueDeserializer the deserializer class used to deserialize the value.
     * @param <V>               the value type.
     * @return A {@link MessageDeserializer} that deserialize the value with the given deserializer.
     */
    static <V> MessageDeserializer<V> valueOnly(DeserializationSchema<V> valueDeserializer) {
        return new MessageDeserializer<V>() {
            @Override
            public void deserialize(Message<?> message, Collector<V> collector) throws IOException {
                valueDeserializer.deserialize(message.getData(), collector);
            }

            @Override
            public boolean isEndOfStream(V nextElement) {
                return valueDeserializer.isEndOfStream(nextElement);
            }

            @Override
            public TypeInformation<V> getProducedType() {
                return valueDeserializer.getProducedType();
            }
        };
    }
}
