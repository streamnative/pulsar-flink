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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.pulsar.source.MessageDeserializer;
import org.apache.flink.connectors.pulsar.source.Partition;
import org.apache.flink.connectors.pulsar.source.PulsarSource;
import org.apache.flink.connectors.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connectors.pulsar.source.StopCondition;
import org.apache.flink.connectors.pulsar.source.offset.SpecifiedStartOffsetInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBaseWithFlink;
import org.apache.flink.streaming.connectors.pulsar.SchemaData;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonDeser;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fooList;
import static org.junit.Assert.assertTrue;

public class PulsarSourceITCase extends PulsarTestBaseWithFlink {
    @Rule
    public RetryRule retryRule = new RetryRule();

    @Before
    public void clearState() {
        SingletonStreamSink.clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test(timeout = 40 * 1000L)
    public void testRunFailedOnWrongServiceUrl() {

        try {

            StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
            see.getConfig().disableSysoutLogging();
            see.setRestartStrategy(RestartStrategies.noRestart());
            see.setParallelism(1);

            PulsarSource<String> source = PulsarSource.builder()
                    .setTopics("tp")
                    .setDeserializer(MessageDeserializer.valueOnly(new SimpleStringSchema()))
                    .stopAt(StopCondition.stopAfterLast())
                    .configure(conf -> conf.set(PulsarSourceOptions.ADMIN_URL, adminUrl))
                    .configurePulsarClient(conf -> conf.setServiceUrl("service"))
                    .build();

            DataStream<String> stream = see.fromSource(source, WatermarkStrategy.noWatermarks(), "pulsar-source");
            stream.print();
            see.execute("wrong service url");
        } catch (Exception e) {
            final Optional<Throwable> optionalThrowable = ExceptionUtils.findThrowableWithMessage(e, "authority component is missing");
            assertTrue(optionalThrowable.isPresent());
            assertTrue(optionalThrowable.get() instanceof PulsarClientException);
        }
    }

    @Test(timeout = 40 * 1000L)
    public void testJson() throws Exception {
        String topic = newTopic();

        sendTypedMessages(topic, SchemaType.JSON, fooList, Optional.empty(), SchemaData.Foo.class);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        see.getConfig().disableSysoutLogging();
        see.setRestartStrategy(RestartStrategies.noRestart());

        PulsarSource<SchemaData.Foo> source = PulsarSource.builder()
                .setTopics(topic)
                .setDeserializer(MessageDeserializer.valueOnly(JsonDeser.of(SchemaData.Foo.class)))
                .stopAt(StopCondition.stopAfterLast())
                .configure(conf -> conf.set(PulsarSourceOptions.ADMIN_URL, adminUrl))
                .configurePulsarClient(conf -> conf.setServiceUrl(serviceUrl))
                .build();

        DataStream<Integer> ds = see.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
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

    @Test(timeout = 40 * 1000L)
    public void testStartFromSpecific() throws Exception {
        String topic = newTopic();
        List<MessageId> mids = sendTypedMessages(topic, SchemaType.STRING, Arrays.asList(
                //  0,   1,   2, 3, 4, 5,  6,  7,  8
                "-20", "-21", "-22", "1", "2", "3", "10", "11", "12"), Optional.empty());
        Set<String> expectedData = new HashSet<>();
        //Map<String, Set<String>> expectedData = new HashMap<>();
        expectedData.addAll(Arrays.asList("2", "3", "10", "11", "12"));

        Map<Partition, MessageId> offset = new HashMap<>();
        offset.put(new Partition(topic, Partition.AUTO_KEY_RANGE), mids.get(4));

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(1);

        PulsarSource<String> source = PulsarSource.builder()
                .setTopics(topic)
                .setDeserializer(MessageDeserializer.valueOnly(new SimpleStringSchema()))
                .startAt(new SpecifiedStartOffsetInitializer(offset, mids.get(0), true))
                .stopAt(StopCondition.stopAfterLast())
                .configure(conf -> {
                    conf.set(PulsarSourceOptions.ADMIN_URL, adminUrl);

                })
                .configurePulsarClient(conf -> {
                    conf.setServiceUrl(serviceUrl);
                })
                .build();

        DataStream stream = see.fromSource(source, WatermarkStrategy.noWatermarks(), "source");
        stream.flatMap(new CheckAllMessageExist(expectedData, 5)).setParallelism(1);

        TestUtils.tryExecute(see, "start from specific");
    }

    public static class CheckAllMessageExist extends RichFlatMapFunction<String, String> {
        private final Set<String> expected;
        private final int total;

        private Set<String> current = new HashSet<>();
        private int count = 0;

        public CheckAllMessageExist(Set<String> expected, int total) {
            this.expected = expected;
            this.total = total;
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            current.add(value);
            count++;
            out.collect(value);
            if (count == total) {
                if (expected.size() != current.size()) {
                    throw new RuntimeException("duplicate elements in " + current.toString());
                }
                if (!expected.equals(current)) {
                    throw new RuntimeException("" + expected + "\n" + current);
                }
                throw new SuccessException();
            }
        }
    }


}
