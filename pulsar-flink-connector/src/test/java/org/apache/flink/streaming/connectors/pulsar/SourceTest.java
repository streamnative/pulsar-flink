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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.TestUtils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * test subscription for pulsar source.
 */
public class SourceTest extends PulsarTestBaseWithFlink {

    private String subscriptionName = "test";

    @Before
    public void clearState() {
        CollectSink.VALUES.clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test
    public void testStartFromSubscription() throws Exception {
        String topic = newTopic();
        PulsarAdmin admin = getPulsarAdmin();
        admin.topics().createSubscription(topic, subscriptionName, MessageId.earliest);
        final List<String> data = generateRange(0, 100);
        sendTypedMessages(topic, SchemaType.STRING, data, Optional.empty());
        List<String> actual = collectData(topic, data.size()).get(50, TimeUnit.SECONDS);
        actual.sort(Comparator.comparingInt(Integer::valueOf));
        Assert.assertEquals(data.subList(0, data.size() - 1), actual);
    }

    @Test
    public void testStartFromSubscription2() throws Exception {
        String topic = newTopic();
        PulsarAdmin admin = getPulsarAdmin();

        List<String> data = generateRange(100, 200);
        sendTypedMessages(topic, SchemaType.STRING, data, Optional.empty());
        admin.topics().createSubscription(topic, subscriptionName, MessageId.latest);
        admin.topics().resetCursor(topic, subscriptionName, admin.topics().getLastMessageId(topic), true);
        final CompletableFuture<List<String>> future = collectData(topic, data.size());
        Thread.sleep(10000);
        data = generateRange(0, 100);
        sendTypedMessages(topic, SchemaType.STRING, data, Optional.empty());
        List<String> actual = future.get(50, TimeUnit.SECONDS);
        Assert.assertEquals(data.subList(0, data.size() - 1), actual);
    }

    @Test
    public void testStartFromSubscription3() throws Exception {
        String topic = newTopic();
        PulsarAdmin admin = getPulsarAdmin();

        List<String> data = generateRange(0, 100);
        sendTypedMessages(topic, SchemaType.STRING, data, Optional.empty());
        admin.topics().createSubscription(topic, subscriptionName, MessageId.latest);
        admin.topics().resetCursor(topic, subscriptionName, admin.topics().getLastMessageId(topic), true);
        data = generateRange(100, 200);
        sendTypedMessages(topic, SchemaType.STRING, data, Optional.empty());
        List<String> actual = collectData(topic, data.size())
                .get(50, TimeUnit.SECONDS);
        Assert.assertEquals(data.subList(0, data.size() - 1), actual);
    }

    private List<String> generateRange(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive).boxed().map(Objects::toString).collect(Collectors.toList());
    }

    private CompletableFuture<List<String>> collectData(String topic, int failCount) {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        see.enableCheckpointing(1000);
        see.setRestartStrategy(RestartStrategies.noRestart());
        final Properties properties = new Properties();
        properties.setProperty("topic", topic);
        PulsarDeserializationSchema<String> deserializationSchema = new PulsarDeserializationSchemaWrapper<>(new SimpleStringSchema());
        final FlinkPulsarSource<String> pulsarSource = new FlinkPulsarSource<>(serviceUrl, adminUrl, deserializationSchema, properties);
        pulsarSource.setStartFromSubscription(subscriptionName);
        final CompletableFuture<List<String>> future = CompletableFuture.supplyAsync(() -> {
            try {
                see.addSource(pulsarSource)
                        .map(new FailingIdentityMapper<>(failCount))
                        .addSink(new CollectSink());
                TestUtils.tryExecute(see, "test");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return CollectSink.VALUES;
        });
        return future;
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<String> {

        // must be static
        public static final List<String> VALUES = new ArrayList<>();

        @Override
        public synchronized void invoke(String value) throws Exception {
            VALUES.add(value);
        }
    }
}

