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
