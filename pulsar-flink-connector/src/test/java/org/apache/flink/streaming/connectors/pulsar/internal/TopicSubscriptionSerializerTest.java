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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * test for TopicSubscriptionSerializer.
 */
@RunWith(Parameterized.class)
public class TopicSubscriptionSerializerTest extends TestCase {

    private final TopicSubscriptionSerializer serializer = TopicSubscriptionSerializer.INSTANCE;

    private TopicSubscription topicSubscription;

    public TopicSubscriptionSerializerTest(TopicSubscription topicSubscription) {
        this.topicSubscription = topicSubscription;
    }

    @Parameterized.Parameters(name = "topicSubscription = {0}")
    public static Collection<TopicSubscription> pattern() {
        return Arrays.asList(
                TopicSubscription.builder()
                        .topic("test-topic")
                        .range(SerializableRange.ofFullRange())
                        .subscriptionName(null)
                        .build(),
                TopicSubscription.builder()
                        .topic("test-topic")
                        .range(SerializableRange.ofFullRange())
                        .subscriptionName("test2")
                        .build()
        );
    }

    @Test
    public void testDeSerialize() throws IOException {
        final DataOutputSerializer outputSerializer = new DataOutputSerializer(1024 * 1024);
        serializer.serialize(topicSubscription, outputSerializer);
        final byte[] sharedBuffer = outputSerializer.getSharedBuffer();
        final DataInputDeserializer inputDeserializer = new DataInputDeserializer(sharedBuffer);
        final TopicSubscription deserialize = serializer.deserialize(inputDeserializer);
        Assert.assertEquals(topicSubscription, deserialize);
    }
}
