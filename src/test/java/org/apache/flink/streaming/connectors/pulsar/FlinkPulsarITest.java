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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.pulsar.internal.AvroDeser;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonDeser;
import org.apache.flink.streaming.connectors.pulsar.internal.ReaderThread;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.streaming.connectors.pulsar.testutils.ValidatingExactlyOnceSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.LongRunningProcessStatus.Status;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fooList;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.CLIENT_CACHE_SIZE_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.FAIL_ON_WRITE_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PULSAR_READER_OPTION_KEY_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_MULTI_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_SINGLE_OPTION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pulsar source sink integration tests.
 */
public class FlinkPulsarITest extends PulsarTestBaseWithFlink {

    private static final Logger log = LoggerFactory.getLogger(FlinkPulsarITest.class);

    @Rule
    public RetryRule retryRule = new RetryRule();

	@Before
	public void clearState() {
		SingletonStreamSink.clear();
		FailingIdentityMapper.failedBefore = false;
	}

    @Test
    public void testRunFailedOnWrongServiceUrl() {

        try {
            Properties props = MapUtils.toProperties(Collections.singletonMap(TOPIC_SINGLE_OPTION_KEY, "tp"));

            StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
            see.getConfig().disableSysoutLogging();
            see.setRestartStrategy(RestartStrategies.noRestart());
            see.setParallelism(1);

            FlinkPulsarSource<String> source =
                    new FlinkPulsarSource<String>("sev", "admin", new SimpleStringSchema(), props).setStartFromEarliest();

            DataStream<String> stream = see.addSource(source);
            stream.print();
            see.execute("wrong service url");
        } catch (Exception e) {
            final Optional<Throwable> optionalThrowable = ExceptionUtils.findThrowableWithMessage(e, "authority component is missing");
            assertTrue(optionalThrowable.isPresent());
            assertTrue(optionalThrowable.get() instanceof PulsarClientException);
        }
    }

    @Test
    public void testCaseSensitiveReaderConf() throws Exception {
        String tp = newTopic();
        List<Integer> messages =
                IntStream.range(0, 50).mapToObj(t -> Integer.valueOf(t)).collect(Collectors.toList());

        sendTypedMessages(tp, SchemaType.INT32, messages, Optional.empty());
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();

        Properties props = MapUtils.toProperties(Collections.singletonMap(TOPIC_SINGLE_OPTION_KEY, tp));
        props.setProperty("pulsar.reader.receiverQueueSize", "1000000");

        FlinkPulsarSource<Integer> source =
                new FlinkPulsarSource<Integer>(serviceUrl, adminUrl, new IntegerDeserializer(), props)
                        .setStartFromEarliest();

        DataStream<Integer> stream = see.addSource(source);
        stream.addSink(new DiscardingSink<Integer>());

        final AtomicReference<Throwable> jobError = new AtomicReference<>();
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(see.getStreamGraph());
        JobID jobID = jobGraph.getJobID();

        final Thread runner = new Thread("runner") {
            @Override
            public void run() {
                try {
                    client.setDetached(false);
                    client.submitJob(jobGraph, this.getClass().getClassLoader());
                } catch (Throwable t) {
                    if (!(t instanceof JobCancellationException)) {
                        jobError.set(t);
                    }
                }
            }
        };
        runner.start();

        Thread.sleep(2000);
        Throwable failureCause = jobError.get();

        if (failureCause != null) {
            failureCause.printStackTrace();
            fail("Test failed prematurely with: " + failureCause.getMessage());
        }

        client.cancel(jobID);
        runner.join();

        assertEquals(client.getJobStatus(jobID).get(), JobStatus.CANCELED);
    }

    @Test
    public void testCaseSensitiveProducerConf() throws Exception {
        int numTopic = 5;
        int numElements = 20;

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(1);

        List<String> topics = new ArrayList<>();
        for (int i = 0; i < numTopic; i++) {
            topics.add(newTopic());
        }

        DataStream<Row> stream = see.addSource(new MultiTopicSource(topics, numElements));

        Properties sinkProp = sinkProperties();
        sinkProp.setProperty("pulsar.producer.blockIfQueueFull", "true");
        sinkProp.setProperty("pulsar.producer.maxPendingMessages", "100000");
        sinkProp.setProperty("pulsar.producer.maxPendingMessagesAcrossPartitions", "5000000");
        sinkProp.setProperty("pulsar.producer.sendTimeoutMs", "30000");
        produceIntoPulsar(stream, intRowWithTopicType(), sinkProp);
        see.execute("write with topics");
    }

    @Test
    public void testClientCacheParameterPassedToTasks() throws Exception {
        int numTopic = 5;
        int numElements = 20;

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(3);

        List<String> topics = new ArrayList<>();
        for (int i = 0; i < numTopic; i++) {
            topics.add(newTopic());
        }

        DataStream<Row> stream = see.addSource(new MultiTopicSource(topics, numElements));

        Properties sinkProp = sinkProperties();
        sinkProp.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
        sinkProp.setProperty(CLIENT_CACHE_SIZE_OPTION_KEY, "7");
        stream.addSink(new AssertSink(serviceUrl, adminUrl, 7, sinkProp, intRowWithTopicType()));
        see.execute("write with topics");
    }

    @Test
    public void testProduceConsumeMultipleTopics() throws Exception {
        int numTopic = 5;
        int numElements = 20;

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(1);

        List<String> topics = new ArrayList<>();
        for (int i = 0; i < numTopic; i++) {
            topics.add(newTopic());
        }

        DataStream<Row> stream = see.addSource(new MultiTopicSource(topics, numElements));

        Properties sinkProp = sinkProperties();
        produceIntoPulsar(stream, intRowWithTopicType(), sinkProp);
        see.execute("write with topics");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        Properties sourceProp = sourceProperties();
        sourceProp.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics.toArray(), ','));
        DataStream<Row> stream1 = env.addSource(new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProp).setStartFromEarliest());

        stream1.flatMap(new CountMessageNumberFM(numElements)).setParallelism(1);
        TestUtils.tryExecute(env, "count elements from topics");
    }

    @Test
    public void testCommitOffsetsToPulsar() throws Exception {
        int numTopic = 3;
        List<Integer> messages = IntStream.range(0, 50).boxed().collect(Collectors.toList());
        List<String> topics = new ArrayList<>();
        Map<String, MessageId> expectedIds = new HashMap<>();

        for (int i = 0; i < numTopic; i++) {
            String topic = newTopic();
            topics.add(topic);
            List<MessageId> ids = sendTypedMessages(topic, SchemaType.INT32, messages, Optional.empty());
            expectedIds.put(topic, ids.get(ids.size() - 1));
        }

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(3);
        see.enableCheckpointing(200);

        String subName = UUID.randomUUID().toString();

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics, ','));

        DataStream stream = see.addSource(
                new FlinkPulsarRowSourceSub(subName, serviceUrl, adminUrl, sourceProps).setStartFromEarliest());
        stream.addSink(new DiscardingSink());

        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread runner = new Thread("runner") {
            @Override
            public void run() {
                try {
                    see.execute();
                } catch (Throwable e) {
                    if (!(e instanceof JobCancellationException)) {
                        error.set(e);
                    }
                }
            }
        };
        runner.start();

        Thread.sleep(3000);

        Long deadline = 30000000000L + System.nanoTime();

        boolean gotLast = false;

        do {
            Map<String, MessageId> ids = getCommittedOffsets(topics, subName);
            if (roughEquals(ids, expectedIds)) {
                gotLast = true;
            } else {
                Thread.sleep(100);
            }
        } while (System.nanoTime() < deadline && !gotLast);

        client.cancel(Iterables.getOnlyElement(getRunningJobs(client)));
        runner.join();

        Throwable t = error.get();
        if (t != null) {
            fail("Job failed with an exception " + ExceptionUtils.stringifyException(t));
        }

        assertTrue(gotLast);
    }

    @Test
    public void testAvro() throws Exception {
        String topic = newTopic();

        sendTypedMessages(topic, SchemaType.AVRO, fooList, Optional.empty(), SchemaData.Foo.class);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        see.getConfig().disableSysoutLogging();
        see.setRestartStrategy(RestartStrategies.noRestart());

        String subName = UUID.randomUUID().toString();

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);

        FlinkPulsarSource<SchemaData.Foo> source =
                new FlinkPulsarSource<>(serviceUrl, adminUrl, AvroDeser.of(SchemaData.Foo.class), sourceProps)
                .setStartFromEarliest();

        DataStream<Integer> ds = see.addSource(source)
                    .map(SchemaData.Foo::getI);

        ds.map(new FailingIdentityMapper<>(fooList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test read data of POJO using avro");
        } catch (Exception e) {

        }

        SingletonStreamSink.compareWithList(
                fooList.subList(0, fooList.size() - 1).stream().map(SchemaData.Foo::getI).map(Objects::toString).collect(Collectors.toList()));
    }

    @Test
    public void testJson() throws Exception {
        String topic = newTopic();

        sendTypedMessages(topic, SchemaType.JSON, fooList, Optional.empty(), SchemaData.Foo.class);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        see.getConfig().disableSysoutLogging();
        see.setRestartStrategy(RestartStrategies.noRestart());

        String subName = UUID.randomUUID().toString();

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);

        FlinkPulsarSource<SchemaData.Foo> source =
                new FlinkPulsarSource<>(serviceUrl, adminUrl, JsonDeser.of(SchemaData.Foo.class), sourceProps)
                        .setStartFromEarliest();

        DataStream<Integer> ds = see.addSource(source)
                .map(SchemaData.Foo::getI);

        ds.map(new FailingIdentityMapper<>(fooList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test read data of POJO using JSON");
        } catch (Exception e) {

        }

        SingletonStreamSink.compareWithList(
                fooList.subList(0, fooList.size() - 1).stream().map(SchemaData.Foo::getI).map(Objects::toString).collect(Collectors.toList()));
    }

    @Test
    public void testStartFromEarliest() throws Exception {
        int numTopic = 3;
        List<Integer> messages = IntStream.range(0, 50).boxed().collect(Collectors.toList());
        List<String> topics = new ArrayList<>();
        Map<String, Set<Integer>> expectedData = new HashMap<>();

        for (int i = 0; i < numTopic; i++) {
            String topic = newTopic();
            topics.add(topic);
            sendTypedMessages(topic, SchemaType.INT32, messages, Optional.empty());
            expectedData.put(topic, new HashSet<>(messages));
        }

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(3);

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics, ','));

        DataStream stream = see.addSource(
                new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProps).setStartFromEarliest());

        stream.flatMap(new CheckAllMessageExist(expectedData, 150)).setParallelism(1);
        TestUtils.tryExecute(see, "start from earliest");
    }

    @Test
    public void testStartFromLatest() throws Exception {
        int numTopic = 3;
        List<Integer> messages = IntStream.range(0, 50).boxed().collect(Collectors.toList());
        List<Integer> newMessages = IntStream.range(50, 60).boxed().collect(Collectors.toList());
        Map<String, Set<Integer>> expectedData = new HashMap<>();

        List<String> topics = new ArrayList<>();

        for (int i = 0; i < numTopic; i++) {
            String topic = newTopic();
            topics.add(topic);
            sendTypedMessages(topic, SchemaType.INT32, messages, Optional.empty());
            expectedData.put(topic, new HashSet<>(newMessages));
        }

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(3);

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics, ','));

        DataStream stream = see.addSource(
                new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProps).setStartFromLatest());
        stream.flatMap(new CheckAllMessageExist(expectedData, 30)).setParallelism(1);
        stream.addSink(new DiscardingSink());

        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(see.getStreamGraph());
        JobID consumeJobId = jobGraph.getJobID();

        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread consumerThread = new Thread() {
            @Override
            public void run() {
                try {
                    client.setDetached(false);
                    client.submitJob(jobGraph, FlinkPulsarITest.class.getClassLoader());
                } catch (Throwable e) {
                    if (!ExceptionUtils.findThrowable(e, JobCancellationException.class).isPresent()) {
                        error.set(e);
                    }
                }
            }
        };
        consumerThread.start();

        waitUntilJobIsRunning(client);

        Thread.sleep(3000);

        Thread extraProduceThread = new Thread() {
            @Override
            public void run() {
                try {
                    for (String tp : topics) {
                        sendTypedMessages(tp, SchemaType.INT32, newMessages, Optional.empty());
                    }
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        extraProduceThread.start();

        consumerThread.join();

        Throwable consumerError = error.get();
        assertTrue(ExceptionUtils.findThrowable(consumerError, SuccessException.class).isPresent());
    }

    @Test
    public void testStartFromSpecific() throws Exception {
        String topic = newTopic();
        List<MessageId> mids = sendTypedMessages(topic, SchemaType.INT32, Arrays.asList(
                //  0,   1,   2, 3, 4, 5,  6,  7,  8
                -20, -21, -22, 1, 2, 3, 10, 11, 12), Optional.empty());

        Map<String, Set<Integer>> expectedData = new HashMap<>();
        expectedData.put(topic, new HashSet<>(Arrays.asList(2, 3, 10, 11, 12)));

        Map<String, MessageId> offset = new HashMap<>();
        offset.put(topic, mids.get(3));

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(1);

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);
        DataStream stream = see.addSource(
                new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProps).setStartFromSpecificOffsets(offset));
        stream.flatMap(new CheckAllMessageExist(expectedData, 5)).setParallelism(1);

        TestUtils.tryExecute(see, "start from specific");
    }

    @Test
    public void testStartFromExternalSubscription() throws Exception {
        String topic = newTopic();
        List<MessageId> mids = sendTypedMessages(topic, SchemaType.INT32, Arrays.asList(
                //  0,   1,   2, 3, 4, 5,  6,  7,  8
                -20, -21, -22, 1, 2, 3, 10, 11, 12), Optional.empty());

        String subName = "sub-1";

        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        admin.topics().createSubscription(TopicName.get(topic).toString(), subName, mids.get(3));

        Map<String, Set<Integer>> expectedData = new HashMap<>();
        expectedData.put(topic, new HashSet<>(Arrays.asList(2, 3, 10, 11, 12)));

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(1);

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);
        DataStream stream = see.addSource(
                new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProps).setStartFromSubscription(subName));
        stream.flatMap(new CheckAllMessageExist(expectedData, 5)).setParallelism(1);

        TestUtils.tryExecute(see, "start from specific");

        assertTrue(Sets.newHashSet(admin.topics().getSubscriptions(topic)).contains(subName));

        admin.close();
    }

    @Test
    public void testOne2OneExactlyOnce() throws Exception {
        String topic = newTopic();
        int parallelism = 5;
        int numElementsPerPartition = 1000;
        int totalElements = parallelism * numElementsPerPartition;
        int failAfterElements = numElementsPerPartition / 3;

        List<String> allTopicNames = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            allTopicNames.add(topic + "-partition-" + i);
        }

        createTopic(topic, parallelism, adminUrl);

        generateRandomizedIntegerSequence(
                StreamExecutionEnvironment.getExecutionEnvironment(), topic, parallelism, numElementsPerPartition, true);

        // run the topology that fails and recovers

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setParallelism(parallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.getConfig().disableSysoutLogging();

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(allTopicNames, ','));

        env.addSource(new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProps).setStartFromEarliest())
                .map(new PartitionValidationMapper(parallelism, 1))
                .map(new FailingIdentityMapper<Row>(failAfterElements))
                .addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

        FailingIdentityMapper.failedBefore = false;
        TestUtils.tryExecute(env, "one to one exactly once test");
    }

    @Test
    public void testOne2MultiSource() throws Exception {
        String topic = newTopic();
        int numPartitions = 5;
        int numElementsPerPartition = 1000;
        int totalElements = numPartitions * numElementsPerPartition;
        int failAfterElements = numElementsPerPartition / 3;

        List<String> allTopicNames = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            allTopicNames.add(topic + "-partition-" + i);
        }

        createTopic(topic, numPartitions, adminUrl);

        generateRandomizedIntegerSequence(
                StreamExecutionEnvironment.getExecutionEnvironment(), topic, numPartitions, numElementsPerPartition, true);

        int parallelism = 2;

        // run the topology that fails and recovers

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setParallelism(parallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.getConfig().disableSysoutLogging();

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(allTopicNames, ','));

        env.addSource(new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProps).setStartFromEarliest())
                .map(new PartitionValidationMapper(parallelism, 3))
                .map(new FailingIdentityMapper<Row>(failAfterElements))
                .addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

        FailingIdentityMapper.failedBefore = false;
        TestUtils.tryExecute(env, "One-source-multi-partitions exactly once test");
    }

    @Test
    public void testReadCompactedTopic() throws Exception {
	    String topic = newTopic();
	    String sub = "compacted-sub";
	    String outputTopic = topic + "-output";

	    int numKeys = 3;
	    int numMessagesPerKey = 10;

        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, sub, MessageId.earliest);

        PulsarClient client = PulsarClient.builder()
             .serviceUrl(serviceUrl)
             .build();

        try (Producer<String> producer = client.newProducer(Schema.STRING)
             .topic(topic)
             .create()) {
            for (int j = 0; j < numMessagesPerKey; j++) {
                for (int i = 0; i < numKeys; i++) {
                    producer.newMessage()
                        .key("key-" + i)
                        .value("value-" + i + "-" + j)
                        .send();
                }
            }
        }

	    admin.topics().triggerCompaction(topic);
	    LongRunningProcessStatus status = admin.topics().compactionStatus(topic);
	    while (status.status == Status.RUNNING) {
            TimeUnit.MILLISECONDS.sleep(500);
            status = admin.topics().compactionStatus(topic);
        }

	    assertEquals(Status.SUCCESS, status.status);

	    try (Reader<String> reader = client.newReader(Schema.STRING)
             .topic(topic)
             .startMessageId(MessageId.earliest)
             .readCompacted(true)
             .create()) {

	        for (int i = 0; i < numKeys; i++) {
	            Message<String> msg = reader.readNext();
	            log.info("Received message : {} - {}", msg.getKey(), msg.getValue());
            }
        }

	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(1);

	    Properties sourceProps = sourceProperties();
	    sourceProps.setProperty(
            PULSAR_READER_OPTION_KEY_PREFIX + "readCompacted", "true");
	    sourceProps.setProperty(
            TOPIC_SINGLE_OPTION_KEY, topic);

        Consumer<byte[]> outputConsumer = client.newConsumer()
            .topic(outputTopic)
            .subscriptionName(sub)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe();

        env.addSource(new FlinkPulsarSource<>(serviceUrl, adminUrl, new SimpleStringSchema(), sourceProps)
                .setStartFromEarliest())
            .addSink(new FlinkPulsarSink<>(serviceUrl, adminUrl, Optional.of(outputTopic),
                new Properties(), null, String.class))
            .setParallelism(1);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                TestUtils.tryExecute(env, "read from compacted topic");
            } catch (Exception e) {
                log.warn("Failed to execute a flink job", e);
            }
        });

        final Map<Integer, Integer> result = new HashMap<>();

        for (int i = 0; i < numKeys; i++) {
            Message<byte[]> msg = outputConsumer.receive();

            String value = new String(msg.getValue(), UTF_8);
            String[] parts = value.split("-");

            int keyIdx = Integer.parseInt(parts[1]);
            int valIdx = Integer.parseInt(parts[2]);

            assertEquals(numMessagesPerKey - 1, valIdx);
            assertNull(result.get(keyIdx));
            result.put(keyIdx, valIdx);
        }

        executorService.shutdown();
        outputConsumer.close();
        client.close();
        admin.close();
    }

    @Test
    public void testTaskNumberGreaterThanPartitionNumber() throws Exception {
        String topic = newTopic();
        int numPartitions = 5;
        int numElementsPerPartition = 1000;
        int totalElements = numPartitions * numElementsPerPartition;
        int failAfterElements = numElementsPerPartition / 3;

        List<String> allTopicNames = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            allTopicNames.add(topic + "-partition-" + i);
        }

        createTopic(topic, numPartitions, adminUrl);

        generateRandomizedIntegerSequence(
                StreamExecutionEnvironment.getExecutionEnvironment(), topic, numPartitions, numElementsPerPartition, true);

        int parallelism = 8;

        // run the topology that fails and recovers

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setParallelism(parallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.getConfig().disableSysoutLogging();

        Properties sourceProps = sourceProperties();
        sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(allTopicNames, ','));

        env.addSource(new FlinkPulsarRowSource(serviceUrl, adminUrl, sourceProps).setStartFromEarliest())
                .map(new PartitionValidationMapper(parallelism, 1))
                .map(new FailingIdentityMapper<Row>(failAfterElements))
                .addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

        FailingIdentityMapper.failedBefore = false;
        TestUtils.tryExecute(env, "source task number > partition number");
    }

    @Test
    public void testCancelingOnFullInput() throws Exception {
        String tp = newTopic();
        int parallelism = 3;
        createTopic(tp, parallelism, adminUrl);

        InfiniteStringGenerator generator = new InfiniteStringGenerator(tp);
        generator.start();

        // launch a consumer asynchronously

        AtomicReference<Throwable> jobError = new AtomicReference<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(100);
        env.getConfig().disableSysoutLogging();

        Properties prop = sourceProperties();
        prop.setProperty(TOPIC_SINGLE_OPTION_KEY, tp);
        env.addSource(new FlinkPulsarRowSource(serviceUrl, adminUrl, prop).setStartFromEarliest())
                .addSink(new DiscardingSink<>());

        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
        JobID jobid = jobGraph.getJobID();

        Thread jobRunner = new Thread("program runner thread") {
            @Override
            public void run() {
                try {
                    client.setDetached(false);
                    client.submitJob(jobGraph, getClass().getClassLoader());
                } catch (Throwable e) {
                    jobError.set(e);
                }
            }
        };
        jobRunner.start();

        Thread.sleep(2000);
        Throwable failureCause = jobError.get();

        if (failureCause != null) {
            failureCause.printStackTrace();
            fail("Test failed prematurely with: " + failureCause.getMessage());
        }

        client.cancel(jobid);

        jobRunner.join();

        assertEquals(client.getJobStatus(jobid).get(), JobStatus.CANCELED);

        if (generator.isAlive()) {
            generator.shutdown();
            generator.join();
        } else {
            Throwable t = generator.getError();
            if (t != null) {
                t.printStackTrace();
                fail("Generator failed " + t.getMessage());
            } else {
                fail("Generator failed with no exception");
            }
        }
    }

    @Test
    public void testOnEmptyInput() throws Exception {
        String tp = newTopic();
        int parallelism = 3;
        createTopic(tp, parallelism, adminUrl);

        // launch a consumer asynchronously

        AtomicReference<Throwable> jobError = new AtomicReference<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(100);
        env.getConfig().disableSysoutLogging();

        Properties prop = sourceProperties();
        prop.setProperty(TOPIC_SINGLE_OPTION_KEY, tp);
        env.addSource(new FlinkPulsarRowSource(serviceUrl, adminUrl, prop).setStartFromEarliest())
                .addSink(new DiscardingSink<>());

        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
        JobID jobid = jobGraph.getJobID();

        Thread jobRunner = new Thread("program runner thread") {
            @Override
            public void run() {
                try {
                    client.setDetached(false);
                    client.submitJob(jobGraph, getClass().getClassLoader());
                } catch (Throwable e) {
                    jobError.set(e);
                }
            }
        };
        jobRunner.start();

        Thread.sleep(2000);
        Throwable failureCause = jobError.get();

        if (failureCause != null) {
            failureCause.printStackTrace();
            fail("Test failed prematurely with: " + failureCause.getMessage());
        }

        client.cancel(jobid);

        jobRunner.join();

        assertEquals(client.getJobStatus(jobid).get(), JobStatus.CANCELED);

    }

    public static boolean isCause(
            Class<? extends Throwable> expected,
            Throwable exc) {
        return expected.isInstance(exc) || (
                exc != null && isCause(expected, exc.getCause())
        );
    }

    private static class MultiTopicSource extends RichParallelSourceFunction<Row> {
        private final List<String> topics;
        private final int numElements;
        private final int base;

        public MultiTopicSource(List<String> topics, int numElements, int base) {
            this.topics = topics;
            this.numElements = numElements;
            this.base = base;
        }

        public MultiTopicSource(List<String> topics, int numElements) {
            this(topics, numElements, 0);
        }

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            for (String topic : topics) {
                for (int i = 0; i < numElements; i++) {
                    ctx.collect(intRowWithTopic(i + base, topic));
                }
            }
        }

        public Row intRowWithTopic(int i, String tp) {
            Row r = new Row(2);
            r.setField(0, tp);
            r.setField(1, i);
            return r;
        }

        @Override
        public void cancel() {

        }
    }

    private DataType intRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD("v", DataTypes.INT()));
    }

    private TypeInformation<Row> intRowTypeInfo() {
        return (TypeInformation<Row>) LegacyTypeInfoDataTypeConverter
                .toLegacyTypeInfo(intRowType());
    }

    private DataType intRowWithTopicType() {
        return DataTypes.ROW(
                DataTypes.FIELD(TOPIC_ATTRIBUTE_NAME, DataTypes.STRING()),
                DataTypes.FIELD("v", DataTypes.INT())
        );
    }

    private TypeInformation<Row> intRowWithTopicTypeInfo() {
        return (TypeInformation<Row>) LegacyTypeInfoDataTypeConverter
                .toLegacyTypeInfo(intRowWithTopicType());
    }

    private Properties sinkProperties() {
        Properties props = new Properties();
        props.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
        props.setProperty(FAIL_ON_WRITE_OPTION_KEY, "true");
        return props;
    }

    private Properties sourceProperties() {
        Properties props = new Properties();
        props.setProperty(PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY, "5000");
        return props;
    }

    private void produceIntoPulsar(DataStream<Row> stream, DataType dt, Properties props) {
        props.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
        stream.addSink(new FlinkPulsarRowSink(serviceUrl, adminUrl, Optional.empty(), props, dt));
    }

    private static class AssertSink extends FlinkPulsarRowSink {

        private final int cacheSize;

        public AssertSink(String serviceUrl, String adminUrl, int cacheSize, Properties properties, DataType dataType) {
            super(serviceUrl, adminUrl, Optional.empty(), properties, dataType);
            this.cacheSize = cacheSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            assertEquals(CachedPulsarClient.getCacheSize(), cacheSize);
        }
    }

    private static class CountMessageNumberFM implements FlatMapFunction<Row, Integer> {
        private final int numElements;

        private Map<String, Integer> map;

        private CountMessageNumberFM(int numElements) {
            this.numElements = numElements;
            this.map = new HashMap<>();
        }

        @Override
        public void flatMap(Row value, Collector<Integer> out) throws Exception {
            String topic = (String) value.getField(2);
            Integer old = map.getOrDefault(topic, 0);
            map.put(topic, old + 1);

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                if (entry.getValue() < numElements) {
                    return;
                } else if (entry.getValue() > numElements) {
                    throw new RuntimeException(entry.getKey() + " has " + entry.getValue() + "elements");
                }
            }
            throw new SuccessException();
        }
    }

    private static Map<String, MessageId> getCommittedOffsets(List<String> topics, String prefix) {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            Map<String, MessageId> results = new HashMap<>();
            String subName = "flink-pulsar-" + prefix;

            for (String topic : topics) {
                int index = TopicName.get(topic).getPartitionIndex();
                MessageId mid = null;

                try {
                    PersistentTopicInternalStats.CursorStats cursor = admin.topics().getInternalStats(topic).cursors.get(subName);
                    if (cursor != null) {
                        String[] le = cursor.readPosition.split(":");
                        long ledgerId = Long.parseLong(le[0]);
                        long entryId = Long.parseLong(le[1]);
                        mid = new MessageIdImpl(ledgerId, entryId, index);
                    }
                } catch (PulsarAdminException e) {
                    // do nothing
                }
                results.put(topic, mid);
            }
            return results;
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean roughEquals(Map<String, MessageId> a, Map<String, MessageId> b) {
        for (Map.Entry<String, MessageId> aE : a.entrySet()) {
            MessageId bmid = b.getOrDefault(aE.getKey(), MessageId.latest);
            if (!ReaderThread.messageIdRoughEquals(bmid, aE.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static void createTopic(String topic, int partition, String adminUrl) {
        assert partition > 1;
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            admin.topics().createPartitionedTopic(topic, partition);
        } catch (PulsarClientException | PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check if all message exist.
     */
    public static class CheckAllMessageExist extends RichFlatMapFunction<Row, Row> {
        private final Map<String, Set<Integer>> expected;
        private final int total;

        private Map<String, List<Integer>> map = new HashMap<>();
        private int count = 0;

        public CheckAllMessageExist(Map<String, Set<Integer>> expected, int total) {
            this.expected = expected;
            this.total = total;
        }

        @Override
        public void flatMap(Row value, Collector<Row> out) throws Exception {
            String topic = (String) value.getField(2);
            int v = Integer.parseInt(value.getField(0).toString());
            List<Integer> current = map.getOrDefault(topic, new ArrayList<>());
            current.add(v);
            map.put(topic, current);

            count++;

            if (count == total) {
                for (Map.Entry<String, List<Integer>> e : map.entrySet()) {
                    Set<Integer> s = new HashSet<>(e.getValue());
                    if (s.size() != e.getValue().size()) {
                        throw new RuntimeException("duplicate elements in " + topic + " " + e.getValue().toString());
                    }
                    Set<Integer> expectedSet = expected.getOrDefault(e.getKey(), null);
                    if (expectedSet == null) {
                        throw new RuntimeException("Unknown topic seen " + e.getKey());
                    } else {
                        if (!expectedSet.equals(s)) {
                            throw new RuntimeException("" + expectedSet + "\n" + s);
                        }
                    }
                }
                throw new SuccessException();
            }
        }
    }

    private void generateRandomizedIntegerSequence(
            StreamExecutionEnvironment see,
            String tp,
            int numPartitions,
            int numElements,
            boolean randomizedOrder) throws Exception {
        see.setParallelism(numPartitions);
        see.getConfig().disableSysoutLogging();
        see.setRestartStrategy(RestartStrategies.noRestart());

        DataStream stream = see.addSource(new RandomizedIntegerSeq(tp, numPartitions, numElements, randomizedOrder));
        produceIntoPulsar(stream, intRowWithTopicType(), sinkProperties());
        see.execute("scrambles in sequence generator");
    }

    private static class RandomizedIntegerSeq extends RichParallelSourceFunction<Row> {
        private final String tp;
        private final int numPartitions;
        private final int numElements;
        private final boolean randomizedOrder;

        private volatile boolean running = true;

        private RandomizedIntegerSeq(String tp, int numPartitions, int numElements, boolean randomizedOrder) {
            this.tp = tp;
            this.numPartitions = numPartitions;
            this.numElements = numElements;
            this.randomizedOrder = randomizedOrder;
        }

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            int subIndex = getRuntimeContext().getIndexOfThisSubtask();
            int all = getRuntimeContext().getNumberOfParallelSubtasks();

            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < numElements; i++) {
                rows.add(intRowWithTopic(subIndex + i * all, topicName(tp, subIndex)));
            }

            if (randomizedOrder) {
                Random rand = new Random();
                for (int i = 0; i < numElements; i++) {
                    int otherPos = rand.nextInt(numElements);
                    Row tmp = rows.get(i);
                    rows.set(i, rows.get(otherPos));
                    rows.set(otherPos, tmp);
                }
            }

            rows.forEach(r -> ctx.collect(r));
        }

        @Override
        public void cancel() {
            running = false;
        }

        String topicName(String tp, int index) {
            return tp + "-partition-" + index;
        }

        Row intRowWithTopic(int i, String tp) {
            Row r = new Row(2);
            r.setField(0, tp);
            r.setField(1, i);
            return r;
        }
    }

    private static class PartitionValidationMapper implements MapFunction<Row, Row> {
        private final int numPartitions;
        private final int maxPartitions;

        private HashSet<String> myTopics;

        private PartitionValidationMapper(int numPartitions, int maxPartitions) {
            this.numPartitions = numPartitions;
            this.maxPartitions = maxPartitions;
            myTopics = new HashSet<>();
        }

        @Override
        public Row map(Row value) throws Exception {
            String topic = (String) value.getField(2);
            myTopics.add(topic);
            if (myTopics.size() > maxPartitions) {
                throw new Exception(String.format(
                        "Error: Elements from too many different partitions: %s, Expected elements only from %d partitions",
                        myTopics.toString(), maxPartitions));
            }
            return value;
        }
    }

    private static class InfiniteStringGenerator extends Thread {
        private final String tp;

        private volatile boolean running = true;
        private volatile Throwable error = null;

        public InfiniteStringGenerator(String tp) {
            this.tp = tp;
        }

        @Override
        public void run() {
            try {
                Properties props = new Properties();
                props.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
                props.setProperty(FAIL_ON_WRITE_OPTION_KEY, "true");

                StreamSink<String> sink = new StreamSink<>(
                        new FlinkPulsarSinkBase<String>(serviceUrl, adminUrl, Optional.of(tp), props, new TopicKeyExtractor<String>() {
                            @Override
                            public byte[] serializeKey(String element) {
                                return new byte[0];
                            }

                            @Override
                            public String getTopic(String element) {
                                return null;
                            }
                        }) {
                            @Override
                            protected Schema<?> getPulsarSchema() {
                                return Schema.STRING;
                            }
                        });

                OneInputStreamOperatorTestHarness<String, Object> testHarness = new OneInputStreamOperatorTestHarness(sink);
                testHarness.open();

                StringBuilder bld = new StringBuilder();
                Random rnd = new Random();

                while (running) {
                    bld.setLength(0);
                    int len = rnd.nextInt(100) + 1;
                    for (int i = 0; i < len; i++) {
                        bld.append((char) (rnd.nextInt(20) + 'a'));
                    }

                    String next = bld.toString();
                    testHarness.processElement(new StreamRecord<>(next));
                }
            } catch (Exception e) {
                this.error = e;
            }
        }

        public void shutdown() {
            this.running = false;
            this.interrupt();
        }

        public Throwable getError() {
            return error;
        }
    }

    private static class IntegerDeserializer implements DeserializationSchema<Integer> {
        private final TypeInformation<Integer> ti;
        private final TypeSerializer<Integer> ser;

        public IntegerDeserializer() {
            this.ti = Types.INT();
            this.ser = ti.createSerializer(new ExecutionConfig());
        }

        @Override
        public Integer deserialize(byte[] message) throws IOException {

            DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(message));
            Integer i = ser.deserialize(in);

            return i;
        }

        @Override
        public boolean isEndOfStream(Integer nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return ti;
        }
    }

    private static class FlinkPulsarRowSourceSub extends FlinkPulsarRowSource {

        private final String sub;

        public FlinkPulsarRowSourceSub(String sub, String serviceUrl, String adminUrl, Properties properties) {
            super(serviceUrl, adminUrl, properties);
            this.sub = sub;
        }

        @Override
        protected String getSubscriptionName() {
            return "flink-pulsar-" + sub;
        }
    }
}
