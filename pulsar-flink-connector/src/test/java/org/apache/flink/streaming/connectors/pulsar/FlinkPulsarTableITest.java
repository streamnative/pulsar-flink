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
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.PulsarTableTestUtils;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;
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

import java.text.MessageFormat;
import java.time.LocalDateTime;
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
import static org.apache.flink.table.utils.TableTestMatchers.deepEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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

        List<String> columns = new ArrayList<>();
        for (TableColumn tableColumn : tSchema.getTableColumns()) {
            final String column = MessageFormat.format(" `{0}` {1}",
                    tableColumn.getName(),
                    tableColumn.getType().getLogicalType().asSerializableString());
            columns.add(column);
        }

        tEnv.executeSql(createTableSql(tableName, table, tSchema, "atomic")).print();

        Table t = tEnv.scan(tableName).select("value");

        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<>(BOOLEAN_LIST.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            see.execute("basic functionality");
        } catch (Exception e) {

        }

        SingletonStreamSink.compareWithList(
                BOOLEAN_LIST.subList(0, BOOLEAN_LIST.size() - 1).stream().map(Objects::toString)
                        .collect(Collectors.toList()));
    }

    @Test(timeout = 40 * 1000L)
    public void testWriteThenRead() throws Exception {
        String tp = newTopic();
        String tableName = TopicName.get(tp).getLocalName();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        DataStreamSource ds = see.fromCollection(fooList);
        ds.addSink(
                new FlinkPulsarSink(
                        serviceUrl, adminUrl, Optional.of(tp), getSinkProperties(),
                        new PulsarSerializationSchemaWrapper.Builder<>(
                                (SerializationSchema<SchemaData.Foo>) element -> {
                                    JSONSchema<SchemaData.Foo> jsonSchema = JSONSchema.of(SchemaData.Foo.class);
                                    return jsonSchema.encode(element);
                                })
                                .usePojoMode(SchemaData.Foo.class, RecordSchemaType.JSON)
                                .build()));

        see.execute("write first");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        TableSchema tSchema = getTableSchema(tp);

        tEnv.executeSql(createTableSql(tableName, tp, tSchema, "json")).print();
        Table t = tEnv.sqlQuery("select i, f, bar from " + tableName);
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(fooList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        try {
            env.execute("count elements from topics");
        } catch (Exception e) {

        }
        SingletonStreamSink.compareWithList(
                fooList.subList(0, fooList.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
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
        tEnv.executeSql(createTableSql(tableName, table, tSchema, "json")).print();

        Table t = tEnv.scan(tableName).select("i, f, bar");
        tEnv.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<Row>(fooList.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        TestUtils.tryExecute(see, "test struct in avro");
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
        tEnv.executeSql(createTableSql(tableName, table, tSchema, "json")).print();

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
        PulsarMetadataReader reader =
                new PulsarMetadataReader(adminUrl, new ClientConfigurationData(), "", caseInsensitiveParams, -1, -1);
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

        tEnv.executeSql(createTableSql(tableName, table, tSchema, "json")).print();

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

        tEnv.executeSql(createTableSql(tableName, table, tSchema, "json")).print();

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

    @Test
    public void testSimpleSQLWork() throws Exception {
        testSimpleSQL(JSON_FORMAT);
        testSimpleSQL(AVRO_FORMAT);
    }

    public void testSimpleSQL(String format) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String topic = newTopic();
        final String createTable;
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
                String.format("'format' = '%s'", format));

        tEnv.executeSql(createTable).await();
        String initialValues = "INSERT INTO pulsar\n" +
                "SELECT id, CAST(ts AS TIMESTAMP(3)) \n" +
                "FROM (VALUES (1, '2019-12-12 00:00:01.001001'), \n" +
                "  (2, '2019-12-12 00:00:01.001001'), \n" +
                "  (3, '2019-12-12 00:00:01.001001'), \n" +
                "  (4, '2019-12-12 00:00:01.001001'), \n" +
                "  (5, '2019-12-12 00:00:01.001001'), \n" +
                "  (6, '2019-12-12 00:00:01.001001'))\n" +
                "  AS orders (id, ts)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Pulsar -------------------
        System.out.println("Insert ok");
        String query = "SELECT \n" +
                "  id + 1 \n" +
                "FROM pulsar \n";
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
                "+I(4)", "+I(5)", "+I(6)", "+I(7)");

        assertEquals(expected, TestingSinkFunction.rows);
    }

    @Test
    public void testPulsarSourceSink() throws Exception {
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
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from pulsar -------------------
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

    @Test
    public void testPulsarSourceSinkWithMetadata() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);
        String topic = newTopic();

        // ---------- Produce an event time stream into pulsar -------------------
        final String createTable = String.format(
                "CREATE TABLE pulsar (\n"
                        + "  `physical_1` STRING,\n"
                        + "  `physical_2` INT,\n"
                        // metadata fields are out of order on purpose
                        // offset is ignored because it might not be deterministic
                        + "  `eventTime` TIMESTAMP(3) METADATA,\n"
                        + "  `properties` MAP<STRING, STRING> METADATA ,\n"
                        + "  `key` STRING,\n"
                        + "  `topic` STRING METADATA VIRTUAL,\n"
                        + "  `sequenceId` BIGINT METADATA VIRTUAL,\n"
                        + "  `physical_3` BOOLEAN\n"
                        + ") WITH (\n"
                        + "  'connector' = 'pulsar',\n"
                        + "  'topic' = '%s',\n"
                        + "  'service-url' = '%s',\n"
                        + "  'admin-url' = '%s',\n"
                        + " 'scan.startup.mode' = 'earliest',\n"
                        + " 'key.format' = 'raw',\n"
                        + " 'key.fields' = 'key',\n"
                        + "  %s\n"
                        + ")",
                topic,
                serviceUrl,
                adminUrl,
                " 'format' = 'avro' ");

        tEnv.executeSql(createTable);

        String initialValues = "INSERT INTO pulsar \n"
                + "VALUES\n"
                + " ('data 1', 1, TIMESTAMP '2020-03-08 13:12:11.123', MAP['k11', 'v11', 'k12', 'v12'], 'key1', TRUE),\n"
                + " ('data 2', 2, TIMESTAMP '2020-03-09 13:12:11.123', MAP['k21', 'v21', 'k22', 'v22'], 'key2', FALSE),\n"
                + " ('data 3', 3, TIMESTAMP '2020-03-10 13:12:11.123', MAP['k31', 'v31', 'k32', 'v32'], 'key3', TRUE)";

        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from pulsar -------------------

        final List<Row> result = PulsarTableTestUtils.collectRows(tEnv.sqlQuery("SELECT * FROM pulsar"), 3);

        final Map<String, String> headers1 = new HashMap<>();
        headers1.put("k11", "v11");
        headers1.put("k12", "v12");

        final Map<String, String> headers2 = new HashMap<>();
        headers2.put("k21", "v21");
        headers2.put("k22", "v22");

        final Map<String, String> headers3 = new HashMap<>();
        headers3.put("k31", "v31");
        headers3.put("k32", "v32");

        final List<Row> expected = Arrays.asList(
                Row.of("data 1", 1, LocalDateTime.parse("2020-03-08T13:12:11.123"), headers1, "key1", topic, 0L, true),
                Row.of("data 2", 2, LocalDateTime.parse("2020-03-09T13:12:11.123"), headers2, "key2", topic, 1L, false),
                Row.of("data 3", 3, LocalDateTime.parse("2020-03-10T13:12:11.123"), headers3, "key3", topic, 2L, true)
        );

        assertThat(result, deepEqualTo(expected, true));
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
