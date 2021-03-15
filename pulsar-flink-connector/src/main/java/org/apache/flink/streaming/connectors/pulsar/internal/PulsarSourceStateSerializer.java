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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;

import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

/**
 * Old PulsarSourceState Serializer for flink state.
 */
public class PulsarSourceStateSerializer implements SimpleVersionedSerializer<Tuple2<TopicRange, MessageId>>, Serializable {

    private static final int CURRENT_VERSION = 0;

    private final ExecutionConfig executionConfig;

    public PulsarSourceStateSerializer(ExecutionConfig executionConfig) {
        this.executionConfig = executionConfig;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Tuple2<TopicRange, MessageId> obj) throws IOException {
        throw new UnsupportedEncodingException("for Pulsar source state migration only");
    }

    @Override
    public Tuple2<TopicRange, MessageId> deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer deserializer = new DataInputDeserializer(serialized);
        try {
            return getV1Serializer().deserialize(deserializer);
        } catch (IOException e) {
            Tuple2<String, MessageId> deserialize = getV0Serializer().deserialize(deserializer);
            return Tuple2.of(new TopicRange(deserialize.f0), deserialize.f1);
        }
    }

    public TupleSerializer<Tuple2<String, MessageId>> getV0Serializer() {
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[]{
                        StringSerializer.INSTANCE,
                        new KryoSerializer<>(MessageId.class, executionConfig)
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<String, MessageId>> tupleClass =
                (Class<Tuple2<String, MessageId>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }

    public TupleSerializer<Tuple2<TopicRange, MessageId>> getV1Serializer() {
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[]{
                        new KryoSerializer<>(TopicRange.class, executionConfig),
                        new KryoSerializer<>(MessageId.class, executionConfig)
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<TopicRange, MessageId>> tupleClass =
                (Class<Tuple2<TopicRange, MessageId>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }
}
