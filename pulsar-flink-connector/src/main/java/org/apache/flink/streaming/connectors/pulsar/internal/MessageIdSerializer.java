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

import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;

/**
 * MessageId Serializer for flink state.
 */
public class MessageIdSerializer extends TypeSerializer<MessageId> {

    public static final MessageIdSerializer INSTANCE = new MessageIdSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<MessageId> duplicate() {
        return this;
    }

    @Override
    public MessageId createInstance() {
        return MessageId.earliest;
    }

    @Override
    public MessageId copy(MessageId from) {
        try {
            return MessageId.fromByteArray(from.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("MessageId copy should not throw an exception", e);
        }
    }

    @Override
    public MessageId copy(MessageId from, MessageId reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(MessageId record, DataOutputView target) throws IOException {
        final byte[] bytes = record.toByteArray();
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public MessageId deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return MessageId.fromByteArray(bytes);
    }

    @Override
    public MessageId deserialize(MessageId reuse, DataInputView source) throws IOException {
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
    public TypeSerializerSnapshot<MessageId> snapshotConfiguration() {
        return new MessageIdSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /**
     * Serializer configuration snapshot for compatibility and format evolution.
     */
    @SuppressWarnings("WeakerAccess")
    public static final class MessageIdSerializerSnapshot extends SimpleTypeSerializerSnapshot<MessageId> {

        public MessageIdSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
