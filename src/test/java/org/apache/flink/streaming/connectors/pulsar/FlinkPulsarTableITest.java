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

import lombok.val;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Pulsar;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.types.Row;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.booleanList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fooList;
import static org.junit.Assert.assertTrue;

public class FlinkPulsarTableITest extends PulsarTestBaseWithFlink {

    @Before
    public void clearState() {
        StreamITCase.testResults().clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test
    public void testBasicFunctioning() throws Exception {
        val see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        val tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();

        sendTypedMessages(table, SchemaType.BOOLEAN, booleanList, Optional.empty());

        tEnv.connect(getPulsarDescriptor(table))
            .inAppendMode()
            .registerTableSource(table);

        Table t = tEnv.scan(table).select("value");
        t.printSchema();

        tEnv.toAppendStream(t, BasicTypeInfo.BOOLEAN_TYPE_INFO)
            .map(new FailingIdentityMapper<>(booleanList.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        TestUtils.tryExecute(see, "basic functionality");

        assertEquals(StreamITCase.testResults(), booleanList.subList(0, booleanList.size() - 1));
    }

    @Test
    public void testWriteThenRead() throws Exception {
        String tp = newTopic();
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(1);

        DataStreamSource ds = see.fromCollection(fooList);
        ds.addSink(
            new FlinkPulsarSink(
                serviceUrl, adminUrl, Optional.of(tp), getSinkProperties(),
                new TopicKeyExtractor<SchemaData.Foo>() {

                    @Override
                    public byte[] serializeKey(SchemaData.Foo element) {
                        return new byte[0];
                    }

                    @Override
                    public String getTopic(SchemaData.Foo element) {
                        return null;
                    }
                },
                SchemaData.Foo.class));

        see.execute("write first");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.setParallelism(1);

        val tEnv = StreamTableEnvironment.create(env);
        tEnv.connect(getPulsarDescriptor(tp))
            .inAppendMode()
            .registerTableSource(tp);

        Table t = tEnv.scan(tp).select("i, f, bar");
        tEnv.toAppendStream(t, Row.class)
            .map(new FailingIdentityMapper<Row>(fooList.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        TestUtils.tryExecute(env, "count elements from topics");
        assertEquals(StreamITCase.testResults(), fooList.subList(0, fooList.size() - 1));
    }

    @Test
    public void testStructTypesInAvro() throws Exception {
        val see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        val tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();

        sendTypedMessages(table, SchemaType.AVRO, fooList, Optional.empty(), SchemaData.Foo.class);

        tEnv
            .connect(getPulsarDescriptor(table))
            .inAppendMode()
            .registerTableSource(table);

        Table t = tEnv.scan(table).select("i, f, bar");
        t.printSchema();
        tEnv.toAppendStream(t, Row.class)
            .map(new FailingIdentityMapper<Row>(fooList.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        TestUtils.tryExecute(see, "test struct in avro");
        assertEquals(StreamITCase.testResults(), fooList.subList(0, fooList.size() - 1));
    }

    private ConnectorDescriptor getPulsarDescriptor(String tableName) {
        return new Pulsar()
            .urls(getServiceUrl(), getAdminUrl())
            .topic(tableName)
            .startFromEarliest()
            .property(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY, "5000");
    }

    private Properties getSinkProperties() {
        Properties properties = new Properties();
        properties.setProperty(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
        properties.setProperty(PulsarOptions.FAIL_ON_WRITE_OPTION_KEY, "true");
        return properties;
    }

    private final AtomicInteger topicId = new AtomicInteger(0);

    private String newTopic() {
        return TopicName.get("topic-" + topicId.getAndIncrement()).toString();
    }
}
