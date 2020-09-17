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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the pulsar messages
 * into data types (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
@PublicEvolving
public interface PulsarDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Method to decide whether the element signals the end of the stream. If
     * true is returned the element won't be emitted.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     *
     * @return True, if the element signals end of stream, false otherwise.
     */
    boolean isEndOfStream(T nextElement);

    /**
     * Deserializes the Pulsar message.
     *
     * @param message Pulsar message to be deserialized.
     *
     * @return The deserialized message as an object (null if the message cannot be deserialized).
     */
    T deserialize(Message message) throws IOException;

    /**
     * Deserializes the Pulsar message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of the
     * produced records should be relatively small. Depending on the source implementation records
     * can be buffered in memory or collecting records might delay emitting checkpoint barrier.
     *
     * @param message The message, as a byte array.
     * @param out The collector to put the resulting messages.
     */
    default void deserialize(Message message, Collector<T> out) throws Exception {
        T deserialized = deserialize(message);
        if (deserialized != null) {
            out.collect(deserialized);
        }
    }
}
