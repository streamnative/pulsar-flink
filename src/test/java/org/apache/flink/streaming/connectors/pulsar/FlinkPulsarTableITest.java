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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Pulsar;
import org.apache.flink.types.Row;

import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.BOOLEAN_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.FLList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.faList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fooList;

/**
 * Table API related Integration tests.
 */
public class FlinkPulsarTableITest extends PulsarTestBaseWithFlink {

    @Before
    public void clearState() {
        SingletonStreamSink.clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test
    public void testBasicFunctioning() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();

        sendTypedMessages(table, SchemaType.BOOLEAN, BOOLEAN_LIST, Optional.empty());

        tEnv.connect(getPulsarDescriptor(table))
                .inAppendMode()
                .registerTableSource(table);

        Table t = tEnv.scan(table).select("value");
        t.printSchema();

        tEnv.toAppendStream(t, BasicTypeInfo.BOOLEAN_TYPE_INFO)
                .map(new FailingIdentityMapper<>(BOOLEAN_LIST.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("basic functionality");
        } catch (Exception e) {

        }

        SingletonStreamSink.compareWithList(
                BOOLEAN_LIST.subList(0, BOOLEAN_LIST.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
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

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.connect(getPulsarDescriptor(tp))
                .inAppendMode()
                .registerTableSource(tp);

        Table t = tEnv.scan(tp).select("i, f, bar");
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(fooList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            env.execute("count elements from topics");
        } catch (Exception e) {

        }
        SingletonStreamSink.compareWithList(fooList.subList(0, fooList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
    }

    @Test
    public void testStructTypesInAvro() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();

        sendTypedMessages(table, SchemaType.AVRO, fooList, Optional.empty(), SchemaData.Foo.class);

        tEnv
                .connect(getPulsarDescriptor(table))
                .inAppendMode()
                .registerTableSource(table);

        Table t = tEnv.scan(table).select("i, f, bar");
        t.printSchema();
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(fooList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test struct in avro");
        } catch (Exception e) {

        }
        SingletonStreamSink.compareWithList(
                fooList.subList(0, fooList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
    }

    @Test
    public void testStructTypesWithJavaList() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();

        sendTypedMessages(table, SchemaType.AVRO, FLList, Optional.empty(), SchemaData.FL.class);

        tEnv
                .connect(getPulsarDescriptor(table))
                .inAppendMode()
                .registerTableSource(table);

        Table t = tEnv.scan(table).select("l");
        t.printSchema();
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(FLList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test struct in avro");
        } catch (Exception e) {

        }
        SingletonStreamSink.compareWithList(
                FLList.subList(0, FLList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
    }

    @Test
    public void testStructTypesWithJavaArray() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();

        sendTypedMessages(table, SchemaType.AVRO, faList, Optional.empty(), SchemaData.FA.class);

        tEnv
                .connect(getPulsarDescriptor(table))
                .inAppendMode()
                .registerTableSource(table);

        Table t = tEnv.scan(table).select("l");
        t.printSchema();
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(faList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test struct in avro");
        } catch (Exception e) {

        }
        SingletonStreamSink.compareWithList(
                faList.subList(0, faList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
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
}
