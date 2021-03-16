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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * TopicSubscription Serializer for flink state.
 */
public class TopicSubscriptionSerializer extends TypeSerializer<TopicSubscription> {

    public static final TopicSubscriptionSerializer INSTANCE = new TopicSubscriptionSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<TopicSubscription> duplicate() {
        return this;
    }

    @Override
    public TopicSubscription createInstance() {
        return new TopicSubscription();
    }

    @Override
    public TopicSubscription copy(TopicSubscription from) {
        return TopicSubscription.builder()
                .topic(from.getTopic())
                .subscriptionName(from.getSubscriptionName())
                .range(from.getRange())
                .build();
    }

    @Override
    public TopicSubscription copy(TopicSubscription from, TopicSubscription reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(TopicSubscription record, DataOutputView target) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(record);
            out.flush();
            final byte[] bytes = baos.toByteArray();
            target.writeInt(bytes.length);
            target.write(bytes);
        }
    }

    @Override
    public TopicSubscription deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] serialized = new byte[length];
        source.readFully(serialized);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream in = new ObjectInputStream(bais)) {
            try {
                return (TopicSubscription) in.readObject();
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public TopicSubscription deserialize(TopicSubscription reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else return obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public TypeSerializerSnapshot<TopicSubscription> snapshotConfiguration() {
        return new TopicSubscriptionSerializer.TopicSubscriptionSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /**
     * Serializer configuration snapshot for compatibility and format evolution.
     */
    @SuppressWarnings("WeakerAccess")
    public static final class TopicSubscriptionSerializerSnapshot extends SimpleTypeSerializerSnapshot<TopicSubscription> {

        public TopicSubscriptionSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
