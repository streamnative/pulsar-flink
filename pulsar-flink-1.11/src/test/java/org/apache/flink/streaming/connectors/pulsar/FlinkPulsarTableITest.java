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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.Atomic;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Pulsar;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Table API related Integration tests.
 */
@Slf4j
public class FlinkPulsarTableITest extends PulsarTestBaseWithFlink {

    private static final String JSON_FORMAT = "json";

    private static final String AVRO_FORMAT = "avro";

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
                .withFormat(new Atomic().setClass(Boolean.class.getCanonicalName()))
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
                        serviceUrl, adminUrl, Optional.of(tp), getSinkProperties(),
                         new PulsarSerializationSchemaWrapper.Builder<>((SerializationSchema<SchemaData.Foo>) element -> {
                             JSONSchema<SchemaData.Foo> jsonSchema = JSONSchema.of(SchemaData.Foo.class);
                             return jsonSchema.encode(element);
                         })
                        .setTopic(tp)
                        .usePojoMode(SchemaData.Foo.class, RecordSchemaType.JSON)
                        .build()));

        see.execute("write first");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        TableSchema tSchema = getTableSchema(tp);

        tEnv.connect(getPulsarDescriptor(tp))
                .withSchema(new Schema().schema(tSchema))
                .withFormat(new Json())
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
    public void testStructTypesInJson() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();
        String tableName = TopicName.get(table).getLocalName();

        sendTypedMessages(table, SchemaType.JSON, fooList, Optional.empty(), SchemaData.Foo.class);
        TableSchema tSchema = getTableSchema(table);

        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .inAppendMode()
                .withFormat(new Json())
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

        sendTypedMessages(table, SchemaType.JSON, flList, Optional.empty(), SchemaData.FL.class);
        TableSchema tSchema = getTableSchema(table);
        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .withFormat(new Json())
                .inAppendMode()
                .createTemporaryTable(tableName);

        Table t = tEnv.scan(tableName).select("l");
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(flList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("test struct in avro");
        } catch (Exception e) {
            log.error("", e);
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

        sendTypedMessages(table, SchemaType.JSON, faList, Optional.empty(), SchemaData.FA.class);
        TableSchema tSchema = getTableSchema(table);

        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .withFormat(new Json())
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

        sendTypedMessages(table, SchemaType.JSON, fmList, Optional.empty(), SchemaData.FM.class);
        TableSchema tSchema = getTableSchema(table);

        tEnv
                .connect(getPulsarDescriptor(table))
                .withSchema(new Schema().schema(tSchema))
                .withFormat(new Json())
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

    /*@Test
    public void testKafkaTableWithMultipleTopics() throws Exception {

        if (isLegacyConnector) {
            return;
        }
        // ---------- create source and sink tables -------------------
        String tableTemp = "create table %s (\n" +
                "  currency string\n" +
                ") with (\n" +
                "  'connector' = '%s',\n" +
                "  'topic' = '%s',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'properties.group.id' = '%s',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  %s\n" +
                ")";
        List<String> currencies = Arrays.asList("Euro", "Dollar", "Yen", "Dummy");
        List<String> topics = currencies.stream()
                .map(currency -> String.format("%s_%s", currency, format))
                .collect(Collectors.toList());
        // Because kafka connector currently doesn't support write data into multiple topic together,
        // we have to create multiple sink tables.
        IntStream.range(0, 4).forEach(index -> {
            createTestTopic(topics.get(index), 1, 1);
            tEnv.executeSql(String.format(
                    tableTemp,
                    currencies.get(index).toLowerCase(),
                    KafkaDynamicTableFactory.IDENTIFIER,
                    topics.get(index),
                    bootstraps,
                    groupId,
                    formatOptions()
            ));
        });
        // create source table
        tEnv.executeSql(
                String.format(
                        tableTemp,
                        "currencies_topic_list",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        String.join(";", topics),
                        bootstraps,
                        groupId,
                        formatOptions()));

        // ---------- Prepare data in Kafka topics -------------------
        String insertTemp = "INSERT INTO %s\n" +
                "SELECT currency\n" +
                " FROM (VALUES ('%s'))\n" +
                " AS orders (currency)";
        currencies.forEach(
                currency -> {
                    try {
                        tEnv.executeSql(String.format(insertTemp, currency.toLowerCase(), currency)).await();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                });

        // ------------- test the topic-list kafka source -------------------
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery("SELECT currency FROM currencies_topic_list"), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(4); // expect to receive 4 records
        result.addSink(sink);

        try {
            env.execute("Job_3");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }
        List<String> expected = Arrays.asList(
                "+I(Dollar)",
                "+I(Dummy)",
                "+I(Euro)",
                "+I(Yen)");
        TestingSinkFunction.rows.sort(Comparator.naturalOrder());
        assertEquals(expected, TestingSinkFunction.rows);

        // ------------- cleanup -------------------
        topics.forEach(KafkaTestBase::deleteTestTopic);
    }*/

    @Test
    public void testSimpleSQLWork() throws Exception {
        testSimpleSQL(true, JSON_FORMAT);
        //testSimpleSQL(true, AVRO_FORMAT);
        //testSimpleSQL(false, JSON_FORMAT);
        //testSimpleSQL(false, AVRO_FORMAT);
    }

    public void testSimpleSQL(boolean isLegacyConnector, String format) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String topic = newTopic() + "_" + isLegacyConnector;
        final String createTable;
        if(!isLegacyConnector){
          createTable = String.format(
                "create table pulsar (\n" +
                        "  id int, \n" +
                        "  compute as id + 1, \n" +
                        "  log_ts timestamp(3),\n" +
                        "  ts as log_ts + INTERVAL '1' SECOND,\n" +
                        "  watermark for ts as ts\n" +
                        ") with (\n" +
                        "  'connector' = 'pulsar',\n" +
                        "  'topic' = '%s',\n" +
                        "  'service-url' = '%s',\n" +
                        "  'admin-url' = '%s',\n" +
                        "  'scan.startup.mode' = 'earliest', \n" +
                        "  %s \n" +
                        ")",
                topic,
                serviceUrl,
                adminUrl,
                formatOptions(isLegacyConnector, format));
        }else{
            createTable = String.format(
                    "create table pulsar (\n" +
                            "  id int, \n" +
                            //"  compute as id + 1, \n" +
                            "  log_ts timestamp(3)\n" +
                            //"  ts as log_ts + INTERVAL '1' SECOND,\n" +
                            //"  watermark for ts as ts\n" +
                            ") with (\n" +
                            "  'connector.type' = 'pulsar', \n" +
                            "  'connector.version' = '1', \n" +
                            "  'connector.topic' = '%s', \n" +
                            "  'connector.service-url' = '%s', \n" +
                            "  'connector.admin-url' = '%s', \n" +
                            "  'connector.use-extend-field' = 'true', \n" +
                            "  'connector.startup-mode' = 'earliest', \n" +
                            "  'update-mode' = 'append', \n" +
                            "  %s \n" +
                            ")",
                    topic,
                    serviceUrl,
                    adminUrl,
                    formatOptions(isLegacyConnector, format));
        }
        tEnv.executeSql(createTable);
        System.out.println("DDL OK");
        Thread.sleep(1000);
        String initialValues = "INSERT INTO pulsar\n" +
                "SELECT id, CAST(ts AS TIMESTAMP(3)) \n" +
                "FROM (VALUES (1, '2019-12-12 00:00:01.001001'), \n" +
                "  (2, '2019-12-12 00:00:01.001001'), \n" +
                "  (3, '2019-12-12 00:00:01.001001'), \n" +
                "  (4, '2019-12-12 00:00:01.001001'), \n" +
                "  (5, '2019-12-12 00:00:01.001001'), \n" +
                "  (6, '2019-12-12 00:00:01.001001'))\n" +
                "  AS orders (id, ts)";
        tEnv.executeSql(initialValues);
        Thread.sleep(1000);
        // ---------- Consume stream from Pulsar -------------------
        System.out.println("Insert ok");
        String query = "SELECT \n" +
                "  id + 1 \n" +
                "FROM pulsar \n";

        //CloseableIterator<Row> collect = tEnv.sqlQuery(query).execute().collect();
        //System.out.println("query ok");
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(6);
        result.addSink(sink).setParallelism(1);

        try {
            see.execute("Job_2");
        } catch (
                Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Pulsar source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }

        List<String> expected = Arrays.asList(
                "+I(2)",
                "+I(3)",
                "+I(4)","+I(5)","+I(6)","+I(7)");

        assertEquals(expected, TestingSinkFunction.rows);
    }

    private String formatOptions(boolean isLegacyConnector, String format) {
        if (!isLegacyConnector) {
            return String.format("'format' = '%s'", format);
        } else {
            String formatType = String.format("'format.type' = '%s'", format);
            if (format.equals(AVRO_FORMAT)) {
                // legacy connector requires to specify avro-schema
                String avroSchema = "{\"type\":\"record\",\"name\":\"row_0\",\"fields\":" +
                        "[{\"name\":\"id\",\"type\":[\"int\"," +
                        "\"null\"]},{\"name\":\"log_ts\",\"type\":{\"type\":\"long\"," +
                        "\"logicalType\":\"timestamp-millis\"}}]}";
                return formatType + String.format(", 'format.avro-schema' = '%s'", avroSchema);
            } else {
                return formatType;
            }
        }
    }

    @Test
    public void testPulsarSourceSink() throws Exception{
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String topic = newTopic();

        final String createTable;
        createTable = String.format(
                "create table pulsar (\n" +
                        "  `computed-price` as price + 1.0,\n" +
                        "  price decimal(38, 18),\n" +
                        "  currency string,\n" +
                        "  log_date date,\n" +
                        "  log_time time(3),\n" +
                        "  log_ts timestamp(3),\n" +
                        "  ts as log_ts + INTERVAL '1' SECOND,\n" +
                        "  watermark for ts as ts\n" +
                        ") with (\n" +
                        "  'connector' = 'pulsar',\n" +
                        "  'topic' = '%s',\n" +
                        "  'service-url' = '%s',\n" +
                        "  'admin-url' = '%s',\n" +
                        "  'scan.startup.mode' = 'earliest', \n" +
                        "  'format' ='avro' \n" +
                        ")",
                topic,
                serviceUrl,
                adminUrl);
        tEnv.executeSql(createTable);
        System.out.println("DDL OK");
        Thread.sleep(1000);
        String initialValues = "INSERT INTO pulsar\n" +
                "SELECT CAST(price AS DECIMAL(10, 2)), currency, " +
                " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n" +
                "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n" +
                "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n" +
                "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n" +
                "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n" +
                "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n" +
                "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n" +
                "  AS orders (price, currency, d, t, ts)";
        tEnv.executeSql(initialValues);
        Thread.sleep(1000);
        // ---------- Consume stream from Kafka -------------------
        System.out.println("Insert ok");
        String query = "SELECT\n" +
                "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n" +
                "  CAST(MAX(log_date) AS VARCHAR),\n" +
                "  CAST(MAX(log_time) AS VARCHAR),\n" +
                "  CAST(MAX(ts) AS VARCHAR),\n" +
                "  COUNT(*),\n" +
                "  CAST(MAX(price) AS DECIMAL(10, 2))\n" +
                "FROM pulsar\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            see.execute("Job_2");
        } catch (
                Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Pulsar source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }

        List<String> expected = Arrays.asList(
                "+I(2019-12-12 00:00:05.000,2019-12-12,00:00:03,2019-12-12 00:00:04.004,3,50.00)",
                "+I(2019-12-12 00:00:10.000,2019-12-12,00:00:05,2019-12-12 00:00:06.006,2,5.33)");

        assertEquals(expected, TestingSinkFunction.rows);
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

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }

    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(RowData value, Context context) {
            rows.add(value.toString());
            if (rows.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }
    }
}
