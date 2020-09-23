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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Pulsar;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.BOOLEAN_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.faList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.flList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fmList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fooList;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_SINGLE_OPTION_KEY;

/**
 * Table API related Integration tests.
 */
public class FlinkPulsarTableITest extends PulsarTestBaseWithFlink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkPulsarTableITest.class);

    @Before
    public void clearState() {
        SingletonStreamSink.clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test(timeout = 40 * 1000L)
    public void testBasicFunctioning() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();
        String tableName = TopicName.get(table).getLocalName();

        sendTypedMessages(table, SchemaType.BOOLEAN, BOOLEAN_LIST, Optional.empty());
        TableSchema tSchema = getTableSchema(table);
        tEnv.connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .inAppendMode()
                .createTemporaryTable(tableName);

        Table t = tEnv.scan(tableName).select("value");

        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<>(BOOLEAN_LIST.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("basic functionality");
        } catch (Exception e) {

        }

        SingletonStreamSink.compareWithList(
                BOOLEAN_LIST.subList(0, BOOLEAN_LIST.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
    }

    @Test(timeout = 40 * 1000L)
    public void testWriteThenRead() throws Exception {
        String tp = newTopic();
        String tableName = TopicName.get(tp).getLocalName();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();
        see.setParallelism(1);

        DataStreamSource ds = see.fromCollection(fooList);
        ds.addSink(
                new FlinkPulsarSink(
                        serviceUrl, adminUrl, Optional.of(tp), getSinkProperties(), TopicKeyExtractor.NULL,
                        SchemaData.Foo.class));

        see.execute("write first");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        TableSchema tSchema = getTableSchema(tp);

        tEnv.connect(getPulsarDescriptor(tp))
                .withSchema(new Schema().schema(tSchema))
                .inAppendMode()
                .createTemporaryTable(tableName);

        Table t = tEnv.scan(tableName).select("i, f, bar");
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(fooList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            env.execute("count elements from topics");
        } catch (Exception e) {

        }
        SingletonStreamSink.compareWithList(fooList.subList(0, fooList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
    }

    @Test(timeout = 40 * 1000L)
    public void testStructTypesInAvro() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();
        String tableName = TopicName.get(table).getLocalName();

        sendTypedMessages(table, SchemaType.AVRO, fooList, Optional.empty(), SchemaData.Foo.class);
        TableSchema tSchema = getTableSchema(table);

        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .inAppendMode()
                .createTemporaryTable(tableName);

        Table t = tEnv.scan(tableName).select("i, f, bar");
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

    @Test(timeout = 40 * 1000L)
    public void testStructTypesWithJavaList() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();
        String tableName = TopicName.get(table).getLocalName();

        sendTypedMessages(table, SchemaType.AVRO, flList, Optional.empty(), SchemaData.FL.class);
        TableSchema tSchema = getTableSchema(table);
        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .inAppendMode()
                .createTemporaryTable(tableName);

        Table t = tEnv.scan(tableName).select("l");
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(flList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test struct in avro");
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        SingletonStreamSink.compareWithList(
                flList.subList(0, flList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
    }

    private TableSchema getTableSchema(String topicName) throws PulsarClientException, PulsarAdminException,
            IncompatibleSchemaException {
        Map<String, String> caseInsensitiveParams = new HashMap<>();
        caseInsensitiveParams.put(TOPIC_SINGLE_OPTION_KEY, topicName);
        PulsarMetadataReader reader = new PulsarMetadataReader(adminUrl, new ClientConfigurationData(), "", caseInsensitiveParams, -1, -1);
        SchemaInfo pulsarSchema = reader.getPulsarSchema(topicName);
        final SimpleSchemaTranslator schemaTranslator = new SimpleSchemaTranslator();
        return schemaTranslator.pulsarSchemaToTableSchema(pulsarSchema);
    }

    @Test(timeout = 40 * 1000L)
    public void testStructTypesWithJavaArray() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();
        String tableName = TopicName.get(table).getLocalName();

        sendTypedMessages(table, SchemaType.AVRO, faList, Optional.empty(), SchemaData.FA.class);
        TableSchema tSchema = getTableSchema(table);

        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .inAppendMode()
                .createTemporaryTable(tableName);

        Table t = tEnv.scan(tableName).select("l");
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

    @Test(timeout = 40 * 1000L)
    public void testStructTypesWithJavaMap() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();
        String tableName = TopicName.get(table).getLocalName();

        sendTypedMessages(table, SchemaType.AVRO, fmList, Optional.empty(), SchemaData.FM.class);
        TableSchema tSchema = getTableSchema(table);

        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .inAppendMode()
                .createTemporaryTable(tableName);

        Table t = tEnv.scan(tableName).select("m");

        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(faList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test struct in avro");
        } catch (Exception e) {

        }
        SingletonStreamSink.compareWithList(
                fmList.subList(0, fmList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
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
