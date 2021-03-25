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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Old PulsarSourceState Serializer for flink state.
 */
@Slf4j
public class PulsarSourceStateSerializer
        implements SimpleVersionedSerializer<Tuple2<TopicSubscription, MessageId>>, Serializable {

    private static final int CURRENT_VERSION = 4;

    private final ExecutionConfig executionConfig;

    private Map<Integer, SerializableFunction<byte[], Tuple2<TopicSubscription, MessageId>>> oldStateSerializer;

    public PulsarSourceStateSerializer(ExecutionConfig executionConfig) {
        this.executionConfig = executionConfig;
        this.oldStateSerializer = new LinkedHashMap<>();
        oldStateSerializer.put(0, bytes -> {
            final DataInputDeserializer deserializer = new DataInputDeserializer(bytes);
            Tuple2<String, MessageId> deserialize = getV0Serializer().deserialize(deserializer);
            TopicSubscription topicSubscription = TopicSubscription.builder()
                    .topic(deserialize.f0)
                    .range(SerializableRange.ofFullRange())
                    .build();
            return Tuple2.of(topicSubscription, deserialize.f1);
        });
        oldStateSerializer.put(1, bytes -> {
            final DataInputDeserializer deserializer = new DataInputDeserializer(bytes);
            Tuple2<TopicRange, MessageId> deserialize = getV1Serializer().deserialize(deserializer);
            TopicSubscription topicSubscription = TopicSubscription.builder()
                    .topic(deserialize.f0.getTopic())
                    .range(deserialize.f0.getRange())
                    .build();
            return Tuple2.of(topicSubscription, deserialize.f1);
        });
        oldStateSerializer.put(2, bytes -> {
            final DataInputDeserializer deserializer = new DataInputDeserializer(bytes);
            Tuple3<TopicRange, MessageId, String> deserialize = getV2Serializer().deserialize(deserializer);
            TopicSubscription topicSubscription = TopicSubscription.builder()
                    .topic(deserialize.f0.getTopic())
                    .range(deserialize.f0.getRange())
                    .subscriptionName(deserialize.f2)
                    .build();
            return Tuple2.of(topicSubscription, deserialize.f1);
        });
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Tuple2<TopicSubscription, MessageId> obj) throws IOException {
        throw new UnsupportedEncodingException("for Pulsar source state migration only");
    }

    @Override
    public Tuple2<TopicSubscription, MessageId> deserialize(int version, byte[] serialized) throws IOException {
        Exception exception = null;
        for (Map.Entry<Integer, SerializableFunction<byte[], Tuple2<TopicSubscription, MessageId>>> entry : oldStateSerializer
                .entrySet()) {
            try {
                final Tuple2<TopicSubscription, MessageId> tuple2 = entry.getValue().apply(serialized);
                log.info("pulsar deser old state " + tuple2);
                return tuple2;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new IllegalArgumentException("not restore Pulsar state", exception);
    }

    public Tuple2<TopicSubscription, MessageId> deserialize(int version, Object oldStateObject) throws IOException {
        final DataOutputSerializer target = new DataOutputSerializer(1024 * 8);
        switch (version) {
            case 0:
                getV0Serializer().serialize((Tuple2<String, MessageId>) oldStateObject, target);
                break;
            case 1:
                getV1Serializer().serialize((Tuple2<TopicRange, MessageId>) oldStateObject, target);
                break;
            case 2:
                getV2Serializer().serialize((Tuple3<TopicRange, MessageId, String>) oldStateObject, target);
                break;
            default:
                throw new IllegalArgumentException("unsupport old pulsar state version");
        }
        return deserialize(version, target.getSharedBuffer());
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

    public TupleSerializer<Tuple3<TopicRange, MessageId, String>> getV2Serializer() {
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[]{
                        new KryoSerializer<>(TopicRange.class, executionConfig),
                        new KryoSerializer<>(MessageId.class, executionConfig),
                        new StringSerializer()
                };
        @SuppressWarnings("unchecked")
        Class<Tuple3<TopicRange, MessageId, String>> tupleClass =
                (Class<Tuple3<TopicRange, MessageId, String>>) (Class<?>) Tuple3.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }

    public TupleSerializer<?> getSerializer(int oldStateVersion) {
        switch (oldStateVersion) {
            case 0:
                return getV0Serializer();
            case 1:
                return getV1Serializer();
            case 2:
                return getV2Serializer();
            default:
                throw new IllegalArgumentException("unsupport old pulsar state version");
        }
    }

    /**
     * Represents a serializable function that accepts one argument and produces a result.
     *
     * @param <T> the type of the input to the function
     * @param <R> the type of the result of the function
     */
    public interface SerializableFunction<T, R> extends Serializable {
        R apply(T param) throws Exception;
    }
}
