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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import junit.framework.TestCase;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * test for MessageIdSerializer.
 */
@RunWith(Parameterized.class)
public class MessageIdSerializerTest extends TestCase {

    private final MessageIdSerializer serializer = MessageIdSerializer.INSTANCE;

    private MessageId messageId;

    public MessageIdSerializerTest(MessageId messageId) {
        this.messageId = messageId;
    }

    @Parameterized.Parameters(name = "messageId = {0}")
    public static Collection<MessageId> pattern() {
        return Arrays.asList(
                MessageId.earliest,
                MessageId.latest,
                new BatchMessageIdImpl(1, 1, 1, 1),
                new MessageIdImpl(1, 1, 1)
        );
    }

    @Test
    public void testDeSerialize() throws IOException {
        final DataOutputSerializer outputSerializer = new DataOutputSerializer(1024 * 1024);
        serializer.serialize(messageId, outputSerializer);
        final byte[] sharedBuffer = outputSerializer.getSharedBuffer();
        final DataInputDeserializer inputDeserializer = new DataInputDeserializer(sharedBuffer);
        final MessageId deserialize = serializer.deserialize(inputDeserializer);
        Assert.assertEquals(messageId, deserialize);
    }
}
