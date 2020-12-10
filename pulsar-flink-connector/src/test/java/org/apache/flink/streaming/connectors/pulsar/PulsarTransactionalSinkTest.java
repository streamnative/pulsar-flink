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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.IntegerSource;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;

import com.fasterxml.jackson.databind.ser.std.StdJdkSerializers;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class PulsarTransactionalSinkTest extends PulsarTestBaseWithFlink{
    private PulsarAdmin admin;
    private final static String CLUSTER_NAME = "standalone";
    private final static String TENANT = "tnx";
    //private final static String NAMESPACE1 = TENANT + "/ns1";

/*    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String TOPIC_OUTPUT = NAMESPACE1 + "/output";
    private final static String TOPIC_MESSAGE_ACK_TEST = NAMESPACE1 + "/message-ack-test";
    private final static String adminUrlStand = "http://localhost:8080";
    private final static String serviceUrlStand = "pulsar://localhost:6650";*/
    /**
     * Tests the exactly-once semantic for the simple writes into Kafka.
     */
    @Test
    public void testExactlyOnceRegularSink() throws Exception {
        admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("app1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        testExactlyOnce(1);
    }

    @Test
    public void testTxn() throws Exception {
        admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("app1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);
        PulsarClient client = PulsarClient.builder().
                serviceUrl(serviceUrl).
                enableTransaction(true).build();
        Thread.sleep(100);
        ((PulsarClientImpl) client)
                .newTransaction()
                .withTransactionTimeout(1, TimeUnit.HOURS)
                .build()
                .get();

    }

    protected void testExactlyOnce(int sinksCount) throws Exception {
        final String topic = "ExactlyOnceTopicSink" + UUID.randomUUID();
        final int numElements = 1000;
        final int failAfterElements = 333;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        // process exactly failAfterElements number of elements and then shutdown Pulsar broker and fail application
        List<Integer> expectedElements = getIntegersSequence(numElements);

        DataStream<Integer> inputStream = env
                .addSource(new IntegerSource(numElements))
                .map(new FailingIdentityMapper<Integer>(failAfterElements));

        for (int i = 0; i < sinksCount; i++) {
            ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
            clientConfigurationData.setServiceUrl(serviceUrl);
            SinkFunction<Integer> sink = new FlinkPulsarSink<>(
                    adminUrl,
                    Optional.of(topic),
                    clientConfigurationData,
                    new Properties(),
                    new PulsarSerializationSchemaWrapper.Builder<>
                            ((SerializationSchema<Integer>) element -> Schema.INT32.encode(element))
                            .useAtomicMode(DataTypes.INT())
                            .build(),
                    PulsarSinkSemantic.EXACTLY_ONCE
            );
            inputStream.addSink(sink);
        }

        FailingIdentityMapper.failedBefore = false;
        TestUtils.tryExecute(env, "Exactly once test");
        for (int i = 0; i < sinksCount; i++) {
            // assert that before failure we successfully snapshot/flushed all expected elements
            assertExactlyOnceForTopic(
                    topic,
                    expectedElements,
                    60000L);
        }

    }

    private List<Integer> getIntegersSequence(int size) {
        List<Integer> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(i);
        }
        return result;
    }

    /**
     * We manually handle the timeout instead of using JUnit's timeout to return failure instead of timeout error.
     * After timeout we assume that there are missing records and there is a bug, not that the test has run out of time.
     */
    public void assertExactlyOnceForTopic(
            String topic,
            List<Integer> expectedElements,
            long timeoutMillis) throws Exception{

        long startMillis = System.currentTimeMillis();
        List<Integer> actualElements = new ArrayList<>();

        // until we timeout...
            PulsarClient client = PulsarClient.builder().enableTransaction(true).serviceUrl(serviceUrl).build();
            Consumer<Integer> test = client
                    .newConsumer(Schema.JSON(Integer.class))
                    .topic(topic)
                    .subscriptionName("test-exactly" + UUID.randomUUID())
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
        while (System.currentTimeMillis() < startMillis + timeoutMillis) {
            // query pulsar for new records ...
            Message<Integer> message = test.receive();
            log.info("consume the message {} with the value {}", message.getMessageId(), message.getValue());
            actualElements.add(message.getValue());
            // succeed if we got all expectedElements
            if (actualElements.equals(expectedElements)) {
                return;
            }
            // fail early if we already have too many elements
            if (actualElements.size() > expectedElements.size()) {
                break;
            }
        }

        fail(String.format("Expected %s, but was: %s", formatElements(expectedElements), formatElements(actualElements)));
    }

    private String formatElements(List<Integer> elements) {
        if (elements.size() > 50) {
            return String.format("number of elements: <%s>", elements.size());
        }
        else {
            return String.format("elements: <%s>", elements);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Mapper that validates partitioning and maps to partition.
     */
    public static class PartitionValidatingMapper extends RichMapFunction<Tuple2<Long, String>, Integer> {

        private final int numPartitions;

        private int ourPartition = -1;

        public PartitionValidatingMapper(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public Integer map(Tuple2<Long, String> value) throws Exception {
            int partition = value.f0.intValue() % numPartitions;
            if (ourPartition != -1) {
                assertEquals("inconsistent partitioning", ourPartition, partition);
            } else {
                ourPartition = partition;
            }
            return partition;
        }
    }

    /**
     * Sink that validates records received from each partition and checks that there are no duplicates.
     */
    public static class PartitionValidatingSink implements SinkFunction<Integer> {
        private final int[] valuesPerPartition;

        public PartitionValidatingSink(int numPartitions) {
            this.valuesPerPartition = new int[numPartitions];
        }

        @Override
        public void invoke(Integer value) throws Exception {
            valuesPerPartition[value]++;

            boolean missing = false;
            for (int i : valuesPerPartition) {
                if (i < 100) {
                    missing = true;
                    break;
                }
            }
            if (!missing) {
                throw new SuccessException();
            }
        }
    }

    private static class BrokerRestartingMapper<T> extends RichMapFunction<T, T>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 6334389850158707313L;

        public static volatile boolean triggeredShutdown;
        public static volatile int lastSnapshotedElementBeforeShutdown;
        public static volatile Runnable shutdownAction;

        private final int failCount;
        private int numElementsTotal;

        private boolean failer;

        public static void resetState(Runnable shutdownAction) {
            triggeredShutdown = false;
            lastSnapshotedElementBeforeShutdown = 0;
            BrokerRestartingMapper.shutdownAction = shutdownAction;
        }

        public BrokerRestartingMapper(int failCount) {
            this.failCount = failCount;
        }

        @Override
        public void open(Configuration parameters) {
            failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
        }

        @Override
        public T map(T value) throws Exception {
            numElementsTotal++;
            Thread.sleep(10);

            if (!triggeredShutdown && failer && numElementsTotal >= failCount) {
                // shut down a Kafka broker
                triggeredShutdown = true;
                shutdownAction.run();
            }
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!triggeredShutdown) {
                lastSnapshotedElementBeforeShutdown = numElementsTotal;
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }
    }

    private static final class InfiniteIntegerSource implements SourceFunction<Integer> {

        private volatile boolean running = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(counter++);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
