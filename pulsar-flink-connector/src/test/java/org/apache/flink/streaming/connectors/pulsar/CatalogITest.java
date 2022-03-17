/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.catalog.PulsarCatalog;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.context.ExecutionContext;
import org.apache.flink.table.client.gateway.context.SessionContext;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INTEGER_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Catalog Integration tests. */
public class CatalogITest extends PulsarTestBaseWithFlink {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogITest.class);

    private static final String INMEMORY_CATALOG = "inmemorycatalog";
    private static final String PULSAR_CATALOG1 = "pulsarcatalog1";
    private static final String PULSAR_CATALOG2 = "pulsarcatalog2";

    private static final String INMEMORY_DB = "mydatabase";
    private static final String PULSAR1_DB = "public/default";
    private static final String PULSAR2_DB = "tn/ns";

    private static final String FLINK_TENANT = "__flink_catalog";

    private static ClusterClient<?> clusterClient;

    @Before
    public void clearStates() {
        SingletonStreamSink.clear();
        FailingIdentityMapper.failedBefore = false;
        clusterClient = flink.getClusterClient();
    }

    @Test(timeout = 40 * 1000L)
    public void testCatalogs() throws Exception {
        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);

        assertEquals(tableEnv.getCurrentCatalog(), INMEMORY_CATALOG);
        assertEquals(tableEnv.getCurrentDatabase(), INMEMORY_DB);

        Catalog catalog = tableEnv.getCatalog(PULSAR_CATALOG1).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof PulsarCatalog);
        tableEnv.useCatalog(PULSAR_CATALOG1);
        assertEquals(tableEnv.getCurrentDatabase(), PULSAR1_DB);

        catalog = tableEnv.getCatalog(PULSAR_CATALOG2).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof PulsarCatalog);
        tableEnv.useCatalog(PULSAR_CATALOG2);
        assertEquals(tableEnv.getCurrentDatabase(), PULSAR2_DB);
    }

    @Test(timeout = 40 * 1000L)
    public void testDatabases() throws Exception {
        List<String> namespaces = Arrays.asList("tn1/ns1", "tn1/ns2");
        List<String> topics = Arrays.asList("tp1", "tp2");
        List<String> topicsFullName =
                topics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());
        List<String> partitionedTopics = Arrays.asList("ptp1", "ptp2");
        List<String> partitionedTopicsFullName =
                partitionedTopics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);

        tableEnv.useCatalog(PULSAR_CATALOG1);
        assertEquals(tableEnv.getCurrentDatabase(), PULSAR1_DB);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getAdminUrl()).build()) {
            admin.tenants()
                    .createTenant(
                            "tn1",
                            TenantInfo.builder()
                                    .adminRoles(Sets.newHashSet())
                                    .allowedClusters(Sets.newHashSet("standalone"))
                                    .build());
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

            List<String> topicsInPulsar = admin.topics().getList("tn1/ns1");
            String[] tables = tableEnv.listTables();

            Set<String> tableSet = Sets.newHashSet(tableEnv.listTables());
            // Pulsar 2.9 introduce TXN support and will create internal topic automatically
            // So we remove the system topic for now to clear the comparison
            // A better approach would be let the PulsarCatalog ignore any internal topic
            // TODO (nlu): let pulsar catalog ignore system owned topic
            tableSet.remove("__transaction_buffer_snapshot");

            assertTrue(
                    Sets.symmetricDifference(
                                    tableSet,
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
        String tableName = newTopic();

        sendTypedMessages(tableName, SchemaType.INT32, INTEGER_LIST, Optional.empty());

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);

        tableEnv.useCatalog(PULSAR_CATALOG1);
        Thread.sleep(2000);

        Table t =
                tableEnv.sqlQuery("select `value` from " + TopicName.get(tableName).getLocalName());

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        streamTableEnvironment
                .toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<>(INTEGER_LIST.size()))
                .setParallelism(1)
                .addSink(new SingletonStreamSink.StringSink<>())
                .setParallelism(1);

        Thread runner =
                new Thread("runner") {
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
                INTEGER_LIST.subList(0, INTEGER_LIST.size() - 1).stream()
                        .map(Objects::toString)
                        .map(s -> "+I[" + s + "]")
                        .collect(Collectors.toList()));
    }

    @Test(timeout = 40 * 1000L)
    public void testTableReadStartFromEarliest() throws Exception {
        String tableName = newTopic();

        sendTypedMessages(tableName, SchemaType.INT32, INTEGER_LIST, Optional.empty());

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);

        tableEnv.useCatalog(PULSAR_CATALOG1);
        tableEnv.getConfig()
                .getConfiguration()
                .setString("table.dynamic-table-options.enabled", "true");

        Table t =
                tableEnv.sqlQuery(
                        "select `value` from "
                                + TopicName.get(tableName).getLocalName()
                                + " /*+ OPTIONS('scan.startup.mode'='earliest') */");

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        streamTableEnvironment
                .toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<>(INTEGER_LIST.size()))
                .setParallelism(1)
                .addSink(new SingletonStreamSink.StringSink<>())
                .setParallelism(1);

        Thread runner =
                new Thread("runner") {
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
                INTEGER_LIST.subList(0, INTEGER_LIST.size() - 1).stream()
                        .map(Objects::toString)
                        .map(s -> "+I[" + s + "]")
                        .collect(Collectors.toList()));
    }

    @Test(timeout = 40 * 10000L)
    public void testTableSink() throws Exception {
        String sourceTopic = newTopic("source");
        String sourceTableName = TopicName.get(sourceTopic).getLocalName();
        sendTypedMessages(sourceTopic, SchemaType.INT32, INTEGER_LIST, Optional.empty());

        String sinkTopic = newTopic("sink");
        String sinkTableName = TopicName.get(sinkTopic).getLocalName();
        getPulsarAdmin().schemas().createSchema(sinkTopic, Schema.INT32.getSchemaInfo());

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);

        tableEnv.useCatalog(PULSAR_CATALOG1);
        tableEnv.getConfig()
                .getConfiguration()
                .setString("table.dynamic-table-options.enabled", "true");

        String insertQ =
                "INSERT INTO "
                        + sinkTableName
                        + " SELECT * FROM `"
                        + sourceTableName
                        + "` /*+ OPTIONS('scan.startup.mode'='earliest') */";

        tableEnv.executeSql(insertQ);

        List<Integer> result = consumeMessage(sinkTableName, Schema.INT32, INTEGER_LIST.size(), 10);

        assertEquals(INTEGER_LIST, result);
    }

    @Test(timeout = 40 * 10000L)
    public void testAvroTableSink() throws Exception {
        String databaseName = newDatabaseName();
        String tableSinkTopic = newTopic("tableSink");
        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        tableEnv.getConfig()
                .addConfiguration(new Configuration().set(CoreOptions.DEFAULT_PARALLELISM, 1));
        registerCatalogs(tableEnv);

        tableEnv.useCatalog(PULSAR_CATALOG1);

        String dbDDL = "CREATE DATABASE " + databaseName;
        tableEnv.executeSql(dbDDL).print();
        tableEnv.executeSql("USE " + databaseName);

        String sinkDDL =
                "create table "
                        + tableSinkName
                        + "(\n"
                        + "  oid STRING,\n"
                        + "  totalprice INT,\n"
                        + "  customerid STRING\n"
                        + ") with (\n"
                        + "   'connector' = 'pulsar',\n"
                        + "   'topic' = '"
                        + tableSinkTopic
                        + "',\n"
                        + "   'format' = 'avro'\n"
                        + ")";
        String insertQ =
                "INSERT INTO "
                        + tableSinkName
                        + " VALUES\n"
                        + "  ('oid1', 10, 'cid1'),\n"
                        + "  ('oid2', 20, 'cid2'),\n"
                        + "  ('oid3', 30, 'cid3'),\n"
                        + "  ('oid4', 10, 'cid4')";

        tableEnv.executeSql(sinkDDL).print();
        tableEnv.executeSql(insertQ);

        List<GenericRecord> result = consumeMessage(tableSinkName, Schema.AUTO_CONSUME(), 4, 10);

        assertEquals(4, result.size());
    }

    @Test(timeout = 40 * 10000L)
    public void testTableSchema() throws Exception {
        String databaseName = newDatabaseName();
        String tableSinkTopic = newTopic("tableSink");
        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();
        String catalogName = "pulsarcatalogtest1";

        TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.fromConfiguration(new Configuration()));

        String createCatalog =
                "CREATE CATALOG "
                        + catalogName
                        + "\n"
                        + "  WITH (\n"
                        + "    'type' = 'pulsar-catalog',\n"
                        + "    'default-database' = 'public/default',\n"
                        + "    'catalog-service-url'='"
                        + getServiceUrl()
                        + "',\n"
                        + "    'catalog-admin-url'=  '"
                        + getAdminUrl()
                        + "'\n"
                        + "  )";
        tableEnv.executeSql(createCatalog);
        tableEnv.useCatalog(catalogName);

        String dbDDL = "CREATE DATABASE " + databaseName;
        tableEnv.executeSql(dbDDL).print();
        tableEnv.executeSql("USE " + databaseName + "");

        String sinkDDL =
                "CREATE TABLE "
                        + tableSinkName
                        + " (\n"
                        + "  `physical_1` STRING,\n"
                        + "  `physical_2` INT,\n"
                        + "  `eventTime` TIMESTAMP(3) METADATA,  \n"
                        + "  `properties` MAP<STRING, STRING> METADATA,\n"
                        + "  `topic` STRING METADATA VIRTUAL,\n"
                        + "  `sequenceId` BIGINT METADATA VIRTUAL,\n"
                        + "  `key` STRING ,\n"
                        + "  `physical_3` BOOLEAN\n"
                        + ")";

        tableEnv.executeSql(sinkDDL).print();
        final TableSchema schema =
                tableEnv.executeSql("DESCRIBE " + tableSinkName).getTableSchema();

        TableEnvironment tableEnv2 =
                TableEnvironment.create(EnvironmentSettings.fromConfiguration(new Configuration()));
        tableEnv2.executeSql(createCatalog);
        tableEnv2.useCatalog(catalogName);
        final TableSchema schema2 =
                tableEnv2.executeSql("DESCRIBE " + tableSinkName).getTableSchema();

        assertEquals(schema, schema2);
    }

    @Test(timeout = 40 * 10000L)
    public void testJsonTableSink() throws Exception {
        String databaseName = newDatabaseName();
        String tableSinkTopic = newTopic("tableSink");
        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);
        tableEnv.useCatalog(PULSAR_CATALOG1);

        String dbDDL = "CREATE DATABASE " + databaseName;
        tableEnv.executeSql(dbDDL).print();
        tableEnv.executeSql("USE " + databaseName + "");

        String sinkDDL =
                "create table "
                        + tableSinkName
                        + "(\n"
                        + "  oid STRING,\n"
                        + "  totalprice INT,\n"
                        + "  customerid STRING\n"
                        + ") with (\n"
                        + "   'connector' = 'pulsar',\n"
                        + "   'topic' = '"
                        + tableSinkTopic
                        + "',\n"
                        + "   'format' = 'json'\n"
                        + ")";
        String insertQ =
                "INSERT INTO "
                        + tableSinkName
                        + " VALUES\n"
                        + "  ('oid1', 10, 'cid1'),\n"
                        + "  ('oid2', 20, 'cid2'),\n"
                        + "  ('oid3', 30, 'cid3'),\n"
                        + "  ('oid4', 10, 'cid4')";

        tableEnv.executeSql(sinkDDL).await(10, TimeUnit.SECONDS);

        tableEnv.executeSql(insertQ);

        List<GenericRecord> result = consumeMessage(tableSinkName, Schema.AUTO_CONSUME(), 4, 10);

        assertEquals(4, result.size());
    }

    @Test(timeout = 40 * 10000L)
    public void testCreateTable() throws Exception {
        String databaseName = newDatabaseName();
        String tableSinkTopic = newTopic("tableSink");
        String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);
        tableEnv.useCatalog(PULSAR_CATALOG1);

        String dbDDL = "CREATE DATABASE " + databaseName;
        tableEnv.executeSql(dbDDL).print();
        tableEnv.executeSql("USE " + databaseName);

        String sinkDDL =
                "create table "
                        + tableSinkName
                        + "(\n"
                        + "  oid STRING,\n"
                        + "  totalprice INT,\n"
                        + "  customerid STRING\n"
                        + ")";
        tableEnv.executeSql(sinkDDL).await(10, TimeUnit.SECONDS);
        assertTrue(Arrays.asList(tableEnv.listTables()).contains(tableSinkName));
    }

    private <T> List<T> consumeMessage(String topic, Schema<T> schema, int count, int timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        final PulsarClient pulsarClient = getPulsarClient();
        return CompletableFuture.supplyAsync(
                        () -> {
                            Consumer<T> consumer = null;
                            try {
                                consumer =
                                        pulsarClient
                                                .newConsumer(schema)
                                                .topic(topic)
                                                .subscriptionInitialPosition(
                                                        SubscriptionInitialPosition.Earliest)
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
                                throw new IllegalStateException(e);
                            } finally {
                                IOUtils.closeQuietly(consumer, i -> {});
                            }
                        })
                .get(timeout, TimeUnit.SECONDS);
    }

    @Test(timeout = 40 * 1000L)
    public void testSinkToExistingTopic() throws Exception {

        String tp = newTopic();
        String tableName = TopicName.get(tp).getLocalName();
        String tableSink = newTopic("tableSink");
        String tableSinkName = TopicName.get(tableSink).getLocalName();

        sendTypedMessages(tp, SchemaType.INT32, INTEGER_LIST, Optional.empty());
        sendTypedMessages(tableSink, SchemaType.INT32, Arrays.asList(-1), Optional.empty());

        ExecutionContext context = createExecutionContext();
        TableEnvironment tableEnv = context.getTableEnvironment();
        registerCatalogs(tableEnv);
        tableEnv.useCatalog(PULSAR_CATALOG1);
        tableEnv.getConfig()
                .getConfiguration()
                .setString("table.dynamic-table-options.enabled", "true");

        String insertQ =
                "INSERT INTO "
                        + tableSinkName
                        + " SELECT * FROM `"
                        + tableName
                        + "` /*+ OPTIONS('scan.startup.mode'='earliest') */";
        tableEnv.executeSql(insertQ);

        List<Integer> expectedOutput = new ArrayList<>();
        expectedOutput.add(-1);
        expectedOutput.addAll(INTEGER_LIST.subList(0, INTEGER_LIST.size() - 2));
        List<Integer> result =
                consumeMessage(tableSinkName, Schema.INT32, expectedOutput.size(), 10);
        assertEquals(expectedOutput, result);
    }

    private ExecutionContext createExecutionContext() throws Exception {
        DefaultContext defaultContext =
                new DefaultContext(
                        new ArrayList<>(),
                        clusterClient.getFlinkConfiguration(),
                        Collections.singletonList(new DefaultCLI()));
        SessionContext sessionContext = SessionContext.create(defaultContext, "test-session");
        return sessionContext.getExecutionContext();
    }

    private void registerCatalogs(TableEnvironment tableEnvironment) {
        tableEnvironment.registerCatalog(
                INMEMORY_CATALOG, new GenericInMemoryCatalog(INMEMORY_CATALOG, INMEMORY_DB));

        tableEnvironment.registerCatalog(
                PULSAR_CATALOG1,
                new PulsarCatalog(
                        PULSAR_CATALOG1,
                        getAdminUrl(),
                        getServiceUrl(),
                        PULSAR1_DB,
                        FLINK_TENANT,
                        null,
                        null));

        tableEnvironment.registerCatalog(
                PULSAR_CATALOG2,
                new PulsarCatalog(
                        PULSAR_CATALOG2,
                        getAdminUrl(),
                        getServiceUrl(),
                        PULSAR2_DB,
                        FLINK_TENANT,
                        null,
                        null));

        tableEnvironment.useCatalog(INMEMORY_CATALOG);
    }

    private String newDatabaseName() {
        return "database" + RandomStringUtils.randomNumeric(8);
    }
}
