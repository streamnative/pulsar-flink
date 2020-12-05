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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.HashSplitSchedulingStrategy;
import org.apache.flink.connector.pulsar.source.KeySharedSplitSchedulingStrategy;
import org.apache.flink.connector.pulsar.source.NoSplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.SplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.SplitSchedulingStrategy;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.StopCondition;
import org.apache.flink.connector.pulsar.source.UniformSplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.subscription.AbstractPulsarSubscriber;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;

import org.apache.pulsar.common.naming.TopicName;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link PulsarSourceEnumerator}.
 */
public class PulsarEnumeratorTest extends PulsarTestBase {
    private static final int NUM_SUBTASKS = 3;
    private static final int NUM_PARTITIONS = 10;
    private static final String DYNAMIC_TOPIC_NAME = "dynamic_topic";
    private static final int NUM_PARTITIONS_DYNAMIC_TOPIC = 4;

    private static final String TOPIC1 = "topic";
    private static final String TOPIC2 = "pattern-topic";
    private static final String NAMESPACE = "public/default";

    private static final int READER0 = 0;
    private static final int READER1 = 1;
    private static final Set<String> PRE_EXISTING_TOPICS =
            new HashSet<>(Arrays.asList(TOPIC1, TOPIC2));
    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
    private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;

    @BeforeClass
    public static void setup() throws Exception {
        pulsarAdmin = getPulsarAdmin();
        pulsarClient = getPulsarClient();
        createTestTopic(TOPIC1, NUM_PARTITIONS);
        createTestTopic(TOPIC2, NUM_PARTITIONS);
    }

    @Test
    public void testStartWithDiscoverPartitionsOnce() throws IOException {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe = createSubscribe(NoSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        try (PulsarSourceEnumerator enumerator = createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY, subscribe)) {
            // Start the enumerator and it should schedule a one time task to discover and assign partitions.
            enumerator.start();
            assertTrue(context.getPeriodicCallables().isEmpty());
            assertEquals("A one time partition discovery callable should have been scheduled",
                    1, context.getOneTimeCallables().size());
        }
    }

    @Test
    public void testStartWithPeriodicPartitionDiscovery() throws IOException {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe = createSubscribe(NoSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        try (PulsarSourceEnumerator enumerator = createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY, subscribe)) {
            // Start the enumerator and it should schedule a one time task to discover and assign partitions.
            enumerator.start();
            assertTrue(context.getOneTimeCallables().isEmpty());
            assertEquals("A periodic partition discovery callable should have been scheduled",
                    1, context.getPeriodicCallables().size());
        }
    }

    @Test
    public void testDiscoverPartitionsTriggersAssignments() throws Throwable {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe = createSubscribe(NoSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        try (PulsarSourceEnumerator enumerator = createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY, subscribe)) {
            // Start the enumerator and it should schedule a one time task to discover and assign partitions.
            enumerator.start();

            // register reader 0.
            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());

            // Run the partition discover callable and check the partition assignment.
            context.runNextOneTimeCallable();

            // Verify assignments for reader 0.
            verifyLastReadersAssignments(context, Arrays.asList(READER0, READER1),
                    PRE_EXISTING_TOPICS, 1, subscribe);
        }
    }

    @Test
    public void testReaderRegistrationTriggersAssignments() throws Throwable {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe = createSubscribe(NoSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        try (PulsarSourceEnumerator enumerator = createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY, subscribe)) {
            // Start the enumerator and it should schedule a one time task to discover and assign partitions.
            enumerator.start();
            context.runNextOneTimeCallable();
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());

            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(context, Collections.singleton(READER0),
                    PRE_EXISTING_TOPICS, 1, subscribe);

            registerReader(context, enumerator, READER1);
            verifyLastReadersAssignments(context, Collections.singleton(READER1),
                    PRE_EXISTING_TOPICS, 2, subscribe);
        }
    }

    @Test
    public void testReaderRegistrationTriggersKeySharedAssignments() throws Throwable {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe = createSubscribe(UniformSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        try (PulsarSourceEnumerator enumerator = createEnumerator(
                context, DISABLE_PERIODIC_PARTITION_DISCOVERY,
                KeySharedSplitSchedulingStrategy.INSTANCE, subscribe)) {
            // Start the enumerator and it should schedule a one time task to discover and assign partitions.
            enumerator.start();
            context.runNextOneTimeCallable();
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());

            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(context, Collections.singleton(READER0),
                    PRE_EXISTING_TOPICS, 1, KeySharedSplitSchedulingStrategy.INSTANCE, subscribe);

            registerReader(context, enumerator, READER1);
            verifyLastReadersAssignments(context, Collections.singleton(READER1),
                    PRE_EXISTING_TOPICS, 2, KeySharedSplitSchedulingStrategy.INSTANCE, subscribe);
        }
    }

    @Test(timeout = 300000L)
    public void testDiscoverPartitionsPeriodically() throws Throwable {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        Set<String> topics = new HashSet<>(PRE_EXISTING_TOPICS);
        topics.add(DYNAMIC_TOPIC_NAME);
        PulsarSubscriber subscribe = createSubscribe(NoSplitDivisionStrategy.INSTANCE, topics);
        PulsarSourceEnumerator enumerator =
                createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY,
                        HashSplitSchedulingStrategy.INSTANCE, subscribe);
        try {
            startEnumeratorAndRegisterReaders(context, enumerator, subscribe);

            // invoke partition discovery callable again and there should be no new assignments.
            context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
            assertEquals("No assignments should be made because there is no partition change",
                    2, context.getSplitsAssignmentSequence().size());

            // create the dynamic topic.
            createTestTopic(DYNAMIC_TOPIC_NAME, NUM_PARTITIONS_DYNAMIC_TOPIC);

            // invoke partition discovery callable again.
            while (true) {
                context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
                if (context.getSplitsAssignmentSequence().size() < 3) {
                    Thread.sleep(10);
                } else {
                    break;
                }
            }
            verifyLastReadersAssignments(
                    context,
                    Arrays.asList(READER0, READER1),
                    Collections.singleton(DYNAMIC_TOPIC_NAME),
                    3, subscribe);
        } finally {
            //we must guarantee topic delete before enumerator close.
            pulsarAdmin.topics().deletePartitionedTopic(DYNAMIC_TOPIC_NAME);
            enumerator.close();
        }
    }

    @Test
    public void testAddSplitsBack() throws Throwable {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe = createSubscribe(NoSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        try (PulsarSourceEnumerator enumerator = createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY, subscribe)) {
            startEnumeratorAndRegisterReaders(context, enumerator, subscribe);

            // Simulate a reader failure.
            context.unregisterReader(READER0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
                    READER0);
            assertEquals("The added back splits should have not been assigned",
                    2, context.getSplitsAssignmentSequence().size());

            // Simulate a reader recovery.
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(context, Collections.singleton(READER0),
                    PRE_EXISTING_TOPICS, 3, subscribe);
        }
    }

    @Test
    public void testWorkWithPreexistingAssignments() throws Throwable {
        final MockSplitEnumeratorContext<PulsarPartitionSplit> context1 = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe1 = createSubscribe(NoSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        Map<Integer, List<PulsarPartitionSplit>> preexistingAssignments;
        try (PulsarSourceEnumerator enumerator = createEnumerator(context1, ENABLE_PERIODIC_PARTITION_DISCOVERY, subscribe1)) {
            startEnumeratorAndRegisterReaders(context1, enumerator, subscribe1);
            preexistingAssignments = asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
        }

        final MockSplitEnumeratorContext<PulsarPartitionSplit> context2 = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        PulsarSubscriber subscribe2 = createSubscribe(NoSplitDivisionStrategy.INSTANCE, PRE_EXISTING_TOPICS);
        try (PulsarSourceEnumerator enumerator =
                     createEnumerator(context2, ENABLE_PERIODIC_PARTITION_DISCOVERY,
                             preexistingAssignments, HashSplitSchedulingStrategy.INSTANCE, subscribe2)) {
            enumerator.start();
            context2.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);

            registerReader(context2, enumerator, READER0);
            assertTrue(context2.getSplitsAssignmentSequence().isEmpty());

            registerReader(context2, enumerator, READER1);
            verifyLastReadersAssignments(context2, Collections.singleton(READER1),
                    PRE_EXISTING_TOPICS, 1, subscribe2);
        }
    }

    // -------------- some common startup sequence ---------------

    private void startEnumeratorAndRegisterReaders(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumerator enumerator,
            PulsarSubscriber subscriber) throws Throwable {
        // Start the enumerator and it should schedule a one time task to discover and assign partitions.
        enumerator.start();

        // register reader 0 before the partition discovery.
        registerReader(context, enumerator, READER0);
        assertTrue(context.getSplitsAssignmentSequence().isEmpty());

        // Run the partition discover callable and check the partition assignment.
        context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
        verifyLastReadersAssignments(context, Collections.singleton(READER0),
                PRE_EXISTING_TOPICS, 1, subscriber);

        // Register reader 1 after first partition discovery.
        registerReader(context, enumerator, READER1);
        verifyLastReadersAssignments(context, Collections.singleton(READER1),
                PRE_EXISTING_TOPICS, 2, subscriber);

    }

    // ----------------------------------------

    private PulsarSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            PulsarSubscriber subscriber) {
        return createEnumerator(enumContext, enablePeriodicPartitionDiscovery,
                HashSplitSchedulingStrategy.INSTANCE, subscriber);
    }

    private PulsarSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            SplitSchedulingStrategy splitSchedulingStrategy,
            PulsarSubscriber subscriber) {
        return createEnumerator(enumContext, enablePeriodicPartitionDiscovery, Collections.emptyMap(),
                splitSchedulingStrategy, subscriber);
    }

    /**
     * Create the enumerator. For the purpose of the tests in this class we don't care about
     * the subscriber and offsets initializer, so just use arbitrary settings.
     */
    private PulsarSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            Map<Integer, List<PulsarPartitionSplit>> currentAssignments,
            SplitSchedulingStrategy splitSchedulingStrategy,
            PulsarSubscriber subscriber) {
        StartOffsetInitializer startingOffsetsInitializer = StartOffsetInitializer.earliest();
        StopCondition stopCondition = StopCondition.never();

        Configuration configuration = getConsumerConfiguration();

        Long partitionDiscoverInterval = enablePeriodicPartitionDiscovery ? 1L : 0L;
        configuration.setLong(PulsarSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS, partitionDiscoverInterval);
        //close pulsarSourceEnumerator will close internal pulsarAdmin, so we must get a new pulsarAdmin here
        // for continue tests.
        pulsarAdmin = getPulsarAdmin();

        return new PulsarSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stopCondition,
                pulsarAdmin,
                configuration,
                enumContext,
                currentAssignments,
                splitSchedulingStrategy);
    }

    // ---------------------

    private PulsarSubscriber createSubscribe(SplitDivisionStrategy splitDivisionStrategy, Set<String> topicsToSubscribe) {
        return PulsarSubscriber.getTopicPatternSubscriber
                (NAMESPACE, splitDivisionStrategy, topicsToSubscribe);
    }

    private void registerReader(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location 0"));
        enumerator.addReader(reader);
    }

    private void verifyLastReadersAssignments(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            Collection<Integer> readers,
            Set<String> topics,
            int expectedAssignmentSeqSize,
            PulsarSubscriber subscriber) throws Exception {
        verifyLastReadersAssignments(context, readers, topics,
                expectedAssignmentSeqSize, HashSplitSchedulingStrategy.INSTANCE, subscriber);
    }

    private void verifyLastReadersAssignments(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            Collection<Integer> readers,
            Set<String> topics,
            int expectedAssignmentSeqSize,
            SplitSchedulingStrategy splitSchedulingStrategy,
            PulsarSubscriber subscriber) throws Exception {
        verifyAssignments(
                getExpectedAssignments(new HashSet<>(readers), topics, splitSchedulingStrategy, subscriber),
                context.getSplitsAssignmentSequence().get(expectedAssignmentSeqSize - 1).assignment());
    }

    private void verifyAssignments(
            Map<Integer, Set<AbstractPartition>> expectedAssignments,
            Map<Integer, List<PulsarPartitionSplit>> actualAssignments) {
        actualAssignments.forEach((reader, splits) -> {
            Set<AbstractPartition> expectedAssignmentsForReader = expectedAssignments.get(reader);
            assertNotNull(expectedAssignmentsForReader);
            assertEquals(expectedAssignmentsForReader.size(), splits.size());
            for (PulsarPartitionSplit split : splits) {
                assertTrue(expectedAssignmentsForReader.contains(split.getPartition()));
            }
        });
    }

    private Map<Integer, Set<AbstractPartition>> getExpectedAssignments(
            Set<Integer> readers,
            Set<String> topics,
            SplitSchedulingStrategy splitSchedulingStrategy,
            PulsarSubscriber subscriber) throws Exception {
        Map<Integer, Set<AbstractPartition>> expectedAssignments = new HashMap<>();
        Collection<AbstractPartition> currentPartitions = ((AbstractPulsarSubscriber) subscriber).getCurrentPartitions(pulsarAdmin);
        for (AbstractPartition tp : currentPartitions) {
            if (topics.stream().anyMatch(prefix -> tp.getTopic().startsWith(TopicName.get(prefix).toString()))) {
                int ownerReader = splitSchedulingStrategy.getIndexOfReader(NUM_SUBTASKS,
                        new PulsarPartitionSplit(tp, StartOffsetInitializer.earliest(), StopCondition.never()));
                if (readers.contains(ownerReader)) {
                    expectedAssignments.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(tp);
                }
            }
        }
        return expectedAssignments;
    }

    private Map<Integer, List<PulsarPartitionSplit>> asEnumState(Map<Integer, List<PulsarPartitionSplit>> assignments) {
        Map<Integer, List<PulsarPartitionSplit>> enumState = new HashMap<>();
        assignments.forEach((reader, assignment) -> enumState.put(reader, new ArrayList<>(assignment)));
        return enumState;
    }

    private static Configuration getConsumerConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(PulsarSourceOptions.ADMIN_URL, adminUrl);
        return configuration;
    }

}
