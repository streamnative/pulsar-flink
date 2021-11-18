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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.pulsar.serialization.PulsarSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;

import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** test exactly-once for pulsar sink. */
public class PulsarSinkExactlyOnceTest extends PulsarTestBaseWithFlink {

    private TestFlinkPulsarSink<String> sinkFunction;
    private String topic;
    private OneInputStreamOperatorTestHarness<String, Object> harness;

    @Before
    public void setUp() throws Exception {
        this.topic = newTopic();
        this.setUpTestHarness();
    }

    @After
    public void tearDown() throws Exception {
        this.closeTestHarness();
    }

    private void setUpTestHarness() throws Exception {
        final Properties properties = new Properties();
        final PulsarSerializationSchemaWrapper<String> schemaWrapper =
                new PulsarSerializationSchemaWrapper.Builder<>(new SimpleStringSchema())
                        .useAtomicMode(DataTypes.STRING())
                        .build();
        this.sinkFunction =
                new TestFlinkPulsarSink<String>(
                        getAdminUrl(),
                        Optional.of(topic),
                        clientConfigurationData,
                        properties,
                        schemaWrapper,
                        PulsarSinkSemantic.EXACTLY_ONCE);
        this.harness =
                new OneInputStreamOperatorTestHarness(
                        new StreamSink(this.sinkFunction), StringSerializer.INSTANCE);
        this.harness.setup();
    }

    private void closeTestHarness() throws Exception {
        this.harness.close();
    }

    @Test
    public void testSubsumedNotificationOfPreviousCheckpoint() throws Exception {
        this.harness.open();
        this.harness.processElement("42", 0L);
        this.harness.snapshot(0L, 1L);
        this.harness.processElement("43", 2L);
        this.harness.snapshot(1L, 3L);
        this.harness.processElement("44", 4L);
        this.harness.snapshot(2L, 5L);
        this.harness.notifyOfCompletedCheckpoint(2L);
        this.harness.notifyOfCompletedCheckpoint(1L);
        assertEquals(Arrays.asList("42", "43", "44"), getActualValues(3));
    }

    @Test
    public void testNotifyOfCompletedCheckpoint() throws Exception {
        this.harness.open();
        this.harness.processElement("42", 0L);
        this.harness.snapshot(0L, 1L);
        this.harness.processElement("43", 2L);
        this.harness.snapshot(1L, 3L);
        this.harness.processElement("44", 4L);
        this.harness.snapshot(2L, 5L);
        this.harness.notifyOfCompletedCheckpoint(1L);
        assertEquals(Arrays.asList("42", "43"), getActualValues(2));
    }

    @Test
    public void testRestoreCheckpoint() throws Exception {
        this.harness.open();
        this.harness.processElement("42", 0L);
        this.harness.snapshot(0L, 1L);
        this.harness.processElement("43", 2L);
        final OperatorSubtaskState snapshot = this.harness.snapshot(1L, 3L);
        this.harness.notifyOfCompletedCheckpoint(1L);

        int count = 100;
        for (int i = 3; i < count; i++) {
            this.harness.processElement(Integer.toString(41 + i), i);
            this.harness.snapshot(i, i);
            this.harness.notifyOfCompletedCheckpoint(i);
        }
        this.closeTestHarness();
        this.setUpTestHarness();
        this.harness.initializeState(snapshot);
        assertEquals(Arrays.asList("42", "43"), getActualValues(2));
    }

    @Test
    public void testFailBeforeNotify() throws Exception {
        this.harness.open();
        this.harness.processElement("42", 0L);
        this.harness.snapshot(0L, 1L);
        this.harness.processElement("43", 2L);
        OperatorSubtaskState snapshot = this.harness.snapshot(1L, 3L);
        this.sinkFunction.setWritable(false);

        try {
            this.harness.processElement("44", 4L);
            this.harness.snapshot(2L, 5L);
            fail("something should fail");
        } catch (NotWritableException ignore) {
        }

        this.closeTestHarness();
        this.sinkFunction.setWritable(true);
        this.setUpTestHarness();
        this.harness.initializeState(snapshot);
        assertEquals(Arrays.asList("42", "43"), getActualValues(2));
        this.closeTestHarness();
    }

    private List<String> getActualValues(int expectedSize) throws Exception {
        final List<String> actualValues = consumeMessage(topic, Schema.STRING, expectedSize, 2000);
        Collections.sort(actualValues);
        return actualValues;
    }

    public <T> List<T> consumeMessage(String topic, Schema<T> schema, int count, int timeout)
            throws TimeoutException, ExecutionException, InterruptedException {
        return CompletableFuture.supplyAsync(
                        () -> {
                            Consumer<T> consumer = null;
                            try {
                                consumer =
                                        getPulsarClient()
                                                .newConsumer(schema)
                                                .topic(topic)
                                                .subscriptionInitialPosition(
                                                        SubscriptionInitialPosition.Earliest)
                                                .subscriptionName("test")
                                                .subscribe();
                                List<T> result = new ArrayList<>(count);
                                for (int i = 0; i < count; i++) {
                                    final Message<T> message = consumer.receive();
                                    result.add(message.getValue());
                                    consumer.acknowledge(message);
                                }
                                consumer.close();
                                return result;
                            } catch (Exception e) {
                                sneakyThrow(e);
                                return null;
                            } finally {
                                IOUtils.closeQuietly(consumer, i -> {});
                            }
                        })
                .get(timeout, TimeUnit.MILLISECONDS);
    }

    /** javac hack for unchecking the checked exception. */
    @SuppressWarnings("unchecked")
    public static <T extends Exception> void sneakyThrow(Exception t) throws T {
        throw (T) t;
    }

    /**
     * mock Pulsar Sink,Support for throwing unwritable exceptions.
     *
     * @param <T> record
     */
    public static class TestFlinkPulsarSink<T> extends FlinkPulsarSink<T> {

        private boolean writable = true;

        public TestFlinkPulsarSink(
                String adminUrl,
                Optional<String> defaultTopicName,
                ClientConfigurationData clientConf,
                Properties properties,
                PulsarSerializationSchema serializationSchema,
                PulsarSinkSemantic semantic) {
            super(
                    adminUrl,
                    defaultTopicName,
                    clientConf,
                    properties,
                    serializationSchema,
                    semantic);
        }

        @Override
        public void invoke(PulsarTransactionState<T> transactionState, T value, Context context)
                throws Exception {
            if (!writable) {
                throw new NotWritableException("TestFlinkPulsarSink");
            }
            super.invoke(transactionState, value, context);
        }

        public void setWritable(boolean writable) {
            this.writable = writable;
        }
    }

    /** not writable exception. */
    public static class NotWritableException extends RuntimeException {
        public NotWritableException(String name) {
            super(String.format("Pulsar [%s] is not writable", name));
        }
    }
}
