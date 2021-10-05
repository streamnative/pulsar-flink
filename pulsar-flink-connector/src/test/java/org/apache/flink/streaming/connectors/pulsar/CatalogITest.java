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

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.testutils.EnvironmentFileUtil;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.pulsar.PulsarCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.ExecutionContext;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INTEGER_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Catalog Integration tests.
 */
public class CatalogITest extends PulsarTestBaseWithFlink {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogITest.class);

    private static final String CATALOGS_ENVIRONMENT_FILE = "test-sql-client-pulsar-catalog.yaml";
    private static final String CATALOGS_ENVIRONMENT_FILE_START = "test-sql-client-pulsar-start-catalog.yaml";

    @Before
    public void clearStates() {
        SingletonStreamSink.clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test(timeout = 40 * 1000L)
    public void testCatalogs() throws Exception {
        String inmemoryCatalog = "inmemorycatalog";
        String pulsarCatalog1 = "pulsarcatalog1";
        String pulsarCatalog2 = "pulsarcatalog2";

        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
        TableEnvironment tableEnv = context.getTableEnvironment();

        assertEquals(tableEnv.getCurrentCatalog(), inmemoryCatalog);
        assertEquals(tableEnv.getCurrentDatabase(), "mydatabase");

        Catalog catalog = tableEnv.getCatalog(pulsarCatalog1).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof PulsarCatalog);
        tableEnv.useCatalog(pulsarCatalog1);
        assertEquals(tableEnv.getCurrentDatabase(), "public/default");

        catalog = tableEnv.getCatalog(pulsarCatalog2).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof PulsarCatalog);
        tableEnv.useCatalog(pulsarCatalog2);
        assertEquals(tableEnv.getCurrentDatabase(), "tn/ns");
    }

    @Test(timeout = 40 * 1000L)
    public void testDatabases() throws Exception {
        String pulsarCatalog1 = "pulsarcatalog1";
        List<String> namespaces = Arrays.asList("tn1/ns1", "tn1/ns2");
        List<String> topics = Arrays.asList("tp1", "tp2");
        List<String> topicsFullName = topics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());
        List<String> partitionedTopics = Arrays.asList("ptp1", "ptp2");
        List<String> partitionedTopicsFullName =
                partitionedTopics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());

        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
        TableEnvironment tableEnv = context.getTableEnvironment();

        tableEnv.useCatalog(pulsarCatalog1);
        assertEquals(tableEnv.getCurrentDatabase(), "public/default");

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getAdminUrl()).build()) {
            admin.tenants().createTenant("tn1",
                    TenantInfo.builder()
                            .adminRoles(Sets.newHashSet())
                            .allowedClusters(Sets.newHashSet("standalone"))
                            .build()
            );
            for (String ns : namespaces) {
                admin.namespaces().createNamespace(ns);
            }

            for (String tp : topicsFullName) {
                admin.topics().createNonPartitionedTopic(tp);
            }

            for (String tp : partitionedTopicsFullName) {
                admin.topics().createPartitionedTopic(tp, 5);
            }

            assertTrue(Sets.newHashSet(tableEnv.listDatabases()).containsAll(namespaces));

            tableEnv.useDatabase("tn1/ns1");

            assertTrue(
                    Sets.symmetricDifference(
                                    Sets.newHashSet(tableEnv.listTables()),
                                    Sets.newHashSet(Iterables.concat(topics, partitionedTopics)))
                            .isEmpty());

        } finally {
            try {
                for (String tp : topicsFullName) {
                    getPulsarAdmin().topics().delete(tp, true);
                }

                for (String tp : partitionedTopicsFullName) {
                    getPulsarAdmin().topics().deletePartitionedTopic(tp, true);
                }

                for (String ns : namespaces) {
                    getPulsarAdmin().namespaces().deleteNamespace(ns, true);
                }
                getPulsarAdmin().tenants().deleteTenant("tn1");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }
    }

    @Test(timeout = 40 * 1000L)
    public void testTableReadStartFromLatestByDefault() throws Exception {
        String pulsarCatalog1 = "pulsarcatalog1";

        String tableName = newTopic();

        sendTypedMessages(tableName, SchemaType.INT32, INTEGER_LIST, Optional.empty());

        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
        TableEnvironment tableEnv = context.getTableEnvironment();

        tableEnv.useCatalog(pulsarCatalog1);
        Thread.sleep(2000);

        Table t = tableEnv.sqlQuery("select `value` from " + TopicName.get(tableName).getLocalName());

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        streamTableEnvironment.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<>(INTEGER_LIST.size())).setParallelism(1)
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        Thread runner = new Thread("runner") {
            @Override
            public void run() {
                try {
                    executionEnvironment.execute("read from latest");
                } catch (Throwable e) {
                    // do nothing
                    LOGGER.error("", e);
                }
            }
        };

        runner.start();

        Thread.sleep(2000);
        sendTypedMessages(tableName, SchemaType.INT32, INTEGER_LIST, Optional.empty());

        Thread.sleep(2000);
        SingletonStreamSink.compareWithList(
                INTEGER_LIST.subList(0, INTEGER_LIST.size() - 1).stream().map(Objects::toString)
                        .collect(Collectors.toList()));
    }

    @Test(timeout = 40 * 1000L)
    public void testTableReadStartFromEarliest() throws Exception {
        String tableName = newTopic();

        sendTypedMessages(tableName, SchemaType.INT32, INTEGER_LIST, Optional.empty());

        Map<String, String> conf = getStreamingConfs();
        conf.put("$VAR_STARTING", "earliest");

        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
        TableEnvironment tableEnv = context.getTableEnvironment();

        tableEnv.useCatalog("pulsarcatalog1");

        Table t = tableEnv.sqlQuery("select `value` from " + TopicName.get(tableName).getLocalName());

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        streamTableEnvironment.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<>(INTEGER_LIST.size())).setParallelism(1)
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        Thread runner = new Thread("runner") {
            @Override
            public void run() {
                try {
                    executionEnvironment.execute("read from earliest");
                } catch (Throwable e) {
                    // do nothing
                    LOGGER.warn("", e);
                }
            }
        };

        runner.start();
        runner.join();

        SingletonStreamSink.compareWithList(
                INTEGER_LIST.subList(0, INTEGER_LIST.size() - 1).stream().map(Objects::toString)
                        .collect(Collectors.toList()));
    }

//    @Test(timeout = 40 * 10000L)
//    public void testTableSink() throws Exception {
//        String tp = newTopic();
//        String tableName = TopicName.get(tp).getLocalName();
//
//        String tableSinkTopic = newTopic("tableSink");
//        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();
//        String pulsarCatalog1 = "pulsarcatalog1";
//
//        sendTypedMessages(tp, SchemaType.INT32, INTEGER_LIST, Optional.empty());
////        getPulsarAdmin().schemas().createSchema(tp, Schema.INT32.getSchemaInfo());
////        getPulsarAdmin().topics().createSubscription(tp, "test", MessageId.earliest);
//        Map<String, String> conf = getStreamingConfs();
//        conf.put("$VAR_STARTING", "earliest");
//
//        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
//        TableEnvironment tableEnv = context.getTableEnvironment();
//
//        tableEnv.useCatalog(pulsarCatalog1);
//
//        String sinkDDL = "create table " + tableSinkName + "(v int)";
//        String insertQ = "INSERT INTO " + tableSinkName + " SELECT * FROM `" + tableName + "`";
//
//        tableEnv.executeSql(sinkDDL).print();
//        tableEnv.executeSql(insertQ);
//
//        List<Integer> result = consumeMessage(tableSinkName, Schema.INT32, INTEGER_LIST.size(), 10);
//
//        assertEquals(INTEGER_LIST, result);
//    }

//    @Test(timeout = 40 * 10000L)
//    public void testAvroTableSink() throws Exception {
//
//        String tableSinkTopic = newTopic("tableSink");
//        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();
//        String pulsarCatalog1 = "pulsarcatalog3";
//
//        Map<String, String> conf = getStreamingConfs();
//        conf.put("$VAR_STARTING", "earliest");
//        conf.put("$VAR_FORMAT", "avro");
//
//        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
//        TableEnvironment tableEnv = context.getTableEnvironment();
//
//        tableEnv.useCatalog(pulsarCatalog1);
//
//        String sinkDDL = "create table " + tableSinkName + "(\n" +
//                "  oid STRING,\n" +
//                "  totalprice INT,\n" +
//                "  customerid STRING\n" +
//                ")";
//        String insertQ = "INSERT INTO " + tableSinkName + " VALUES\n" +
//                "  ('oid1', 10, 'cid1'),\n" +
//                "  ('oid2', 20, 'cid2'),\n" +
//                "  ('oid3', 30, 'cid3'),\n" +
//                "  ('oid4', 10, 'cid4')";
//
//        tableEnv.executeSql(sinkDDL).print();
//        tableEnv.executeSql(insertQ);
//
//        List<GenericRecord> result = consumeMessage(tableSinkName, Schema.AUTO_CONSUME(), 4, 10);
//
//        assertEquals(4, result.size());
//    }

//    @Test(timeout = 40 * 10000L)
//    public void testTableSchema() throws Exception {
//
//        String tableSinkTopic = newTopic("tableSink");
//        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();
//        String useCatalog = "pulsarcatalog4";
//
//        Map<String, String> conf = getStreamingConfs();
//        conf.put("$VAR_STARTING", "earliest");
//        conf.put("$VAR_FORMAT", "avro");
//
//        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
//        TableEnvironment tableEnv = context.getTableEnvironment();
//        tableEnv.useCatalog(useCatalog);
//
//        String sinkDDL = "CREATE TABLE " + tableSinkName + " (\n" +
//                "  `physical_1` STRING,\n" +
//                "  `physical_2` INT,\n" +
//                "  `eventTime` TIMESTAMP(3) METADATA,  \n" +
//                "  `properties` MAP<STRING, STRING> METADATA,\n" +
//                "  `topic` STRING METADATA VIRTUAL,\n" +
//                "  `sequenceId` BIGINT METADATA VIRTUAL,\n" +
//                "  `key` STRING ,\n" +
//                "  `physical_3` BOOLEAN\n" +
//                ")";
//
//        tableEnv.executeSql(sinkDDL).print();
//        final TableSchema schema = tableEnv.executeSql("DESCRIBE " + tableSinkName).getTableSchema();
//
//        ExecutionContext context2 = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
//        TableEnvironment tableEnv2 = context2.getTableEnvironment();
//        tableEnv2.useCatalog(useCatalog);
//        final TableSchema schema2 = tableEnv2.executeSql("DESCRIBE " + tableSinkName).getTableSchema();
//
//        assertEquals(schema, schema2);
//    }

//    @Test(timeout = 40 * 10000L)
//    public void testJsonTableSink() throws Exception {
//
//        String tableSinkTopic = newTopic("tableSink");
//        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();
//        String pulsarCatalog1 = "pulsarcatalog3";
//
//        Map<String, String> conf = getStreamingConfs();
//        conf.put("$VAR_STARTING", "earliest");
//        conf.put("$VAR_FORMAT", "json");
//
//        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
//        TableEnvironment tableEnv = context.getTableEnvironment();
//
//        tableEnv.useCatalog(pulsarCatalog1);
//
//        String sinkDDL = "create table " + tableSinkName + "(\n" +
//                "  oid STRING,\n" +
//                "  totalprice INT,\n" +
//                "  customerid STRING\n" +
//                ")";
//        String insertQ = "INSERT INTO " + tableSinkName + " VALUES\n" +
//                "  ('oid1', 10, 'cid1'),\n" +
//                "  ('oid2', 20, 'cid2'),\n" +
//                "  ('oid3', 30, 'cid3'),\n" +
//                "  ('oid4', 10, 'cid4')";
//
//        tableEnv.executeSql(sinkDDL).print();
//        tableEnv.executeSql(insertQ);
//
//        List<GenericRecord> result = consumeMessage(tableSinkName, Schema.AUTO_CONSUME(), 4, 10);
//
//        assertEquals(4, result.size());
//    }

    @NotNull
    private <T> List<T> consumeMessage(String topic, Schema<T> schema, int count, int timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        final PulsarClient pulsarClient = getPulsarClient();
        return CompletableFuture.supplyAsync(() -> {
            Consumer<T> consumer = null;
            try {
                consumer = pulsarClient.newConsumer(schema)
                        .topic(topic)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
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
                throw new RuntimeException(e);
            } finally {
                IOUtils.closeQuietly(consumer, i -> {
                });
            }
        }).get(timeout, TimeUnit.SECONDS);
    }

    @Test(timeout = 40 * 1000L)
    public void testSinkToExistingTopic() throws Exception {

        String tp = newTopic();
        String tableName = TopicName.get(tp).getLocalName();
        String tableSink = newTopic("tableSink");
        String tableSinkName = TopicName.get(tableSink).getLocalName();

        sendTypedMessages(tp, SchemaType.INT32, INTEGER_LIST, Optional.empty());
        sendTypedMessages(tableSink, SchemaType.INT32, Arrays.asList(-1), Optional.empty());

        Map<String, String> conf = getStreamingConfs();
        conf.put("$VAR_STARTING", "earliest");

        ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
        TableEnvironment tableEnv = context.getTableEnvironment();

        tableEnv.useCatalog("pulsarcatalog1");

        String insertQ = "INSERT INTO " + tableSinkName + " SELECT * FROM `" + tableName + "`";
        tableEnv.executeSql(insertQ);

        List<Integer> expectedOutput = new ArrayList<>();
        expectedOutput.add(-1);
        expectedOutput.addAll(INTEGER_LIST.subList(0, INTEGER_LIST.size() - 2));
        List<Integer> result = consumeMessage(tableSinkName, Schema.INT32, expectedOutput.size(), 10);
        assertEquals(expectedOutput, result);
    }

    private ExecutionContext createExecutionContext(String file, Map<String, String> replaceVars) throws Exception {
        final Environment env = EnvironmentFileUtil.parseModified(
                file,
                replaceVars);
        final Configuration flinkConfig = new Configuration();
        return ExecutionContext.builder(
                env,
                new SessionContext("test-session", new Environment()),
                Collections.emptyList(),
                flinkConfig,
                new DefaultClusterClientServiceLoader(),
                new Options(),
                Collections.singletonList(new DefaultCLI())).build();
    }

    private Map<String, String> getStreamingConfs() {
        Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        replaceVars.put("$VAR_SERVICEURL", getServiceUrl());
        replaceVars.put("$VAR_ADMINURL", getAdminUrl());
        return replaceVars;
    }
}
