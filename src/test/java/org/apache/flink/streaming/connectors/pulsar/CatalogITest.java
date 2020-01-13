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

import com.google.common.collect.Iterables;
import lombok.val;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.pulsar.testutils.EnvironmentFileUtil;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.pulsar.PulsarCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.ExecutionContext;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.int32List;


public class CatalogITest extends PulsarTestBaseWithFlink {
    
    private static final String CATALOGS_ENVIRONMENT_FILE = "test-sql-client-pulsar-catalog.yaml";
    private static final String CATALOGS_ENVIRONMENT_FILE_START = "test-sql-client-pulsar-start-catalog.yaml";
    
    @Before
    public void clearStates() {
        StreamITCase.testResults().clear();
        FailingIdentityMapper.failedBefore = false;
    }
    
    @Test
    public void testCatalogs() throws Exception {
        val inmemoryCatalog = "inmemorycatalog";
        val pulsarCatalog1 = "pulsarcatalog1";
        val pulsarCatalog2 = "pulsarcatalog2";
        
        val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
        val tableEnv = context.createEnvironmentInstance().getTableEnvironment();
        
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
    
    @Test
    public void testDatabases() throws Exception {
        val pulsarCatalog1 = "pulsarcatalog1";
        val namespaces = Arrays.asList("tn1/ns1", "tn1/ns2");
        val topics = Arrays.asList("tp1", "tp2");
        val topicsFullName = topics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());
        val partitionedTopics = Arrays.asList("ptp1", "ptp2");
        val partitionedTopicsFullName = partitionedTopics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());

        val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
        val tableEnv = context.createEnvironmentInstance().getTableEnvironment();

        tableEnv.useCatalog(pulsarCatalog1);
        assertEquals(tableEnv.getCurrentDatabase(), "public/default");

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getAdminUrl()).build()) {
            admin.tenants().createTenant("tn1",
                new TenantInfo(Sets.newHashSet(), Sets.newHashSet("standalone")));
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

            for (String tp : topicsFullName) {
                admin.topics().delete(tp);
            }

            for (String tp : partitionedTopicsFullName) {
                admin.topics().deletePartitionedTopic(tp);
            }

            for (String ns : namespaces) {
                admin.namespaces().deleteNamespace(ns);
            }
        }
    }

    @Test
    public void testTableReadStartFromLatestByDefault() throws Exception {
        val pulsarCatalog1 = "pulsarcatalog1";

        val tableName = newTopic();

        sendTypedMessages(tableName, SchemaType.INT32, int32List, Optional.empty());

        val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
        val tableEnv = context.createEnvironmentInstance().getTableEnvironment();

        tableEnv.useCatalog(pulsarCatalog1);

        Table t = tableEnv.scan(TopicName.get(tableName).getLocalName()).select("value");
        val stream = ((StreamTableEnvironment) ((TableImpl) t).getTableEnvironment()).toAppendStream(t, Row.class);
        stream.map(new FailingIdentityMapper<Row>(int32List.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        val runner = new Thread("runner") {
            @Override
            public void run() {
                try {
                    tableEnv.execute("read from latest");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        runner.start();

        Thread.sleep(2000);
        sendTypedMessages(tableName, SchemaType.INT32, int32List, Optional.empty());

        Thread.sleep(2000);
        assertEquals(StreamITCase.testResults(), int32List.subList(0, int32List.size() - 1));
    }

    @Test
    public void testTableReadStartFromEarliest() throws Exception {
        val tableName = newTopic();

        sendTypedMessages(tableName, SchemaType.INT32, int32List, Optional.empty());

        val conf = getStreamingConfs();
        conf.put("$VAR_STARTING", "earliest");

        val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
        val tableEnv = context.createEnvironmentInstance().getTableEnvironment();

        tableEnv.useCatalog("pulsarCatalog1");

        Table t = tableEnv.scan(TopicName.get(tableName).getLocalName()).select("value");
        val stream = ((StreamTableEnvironment) ((TableImpl) t).getTableEnvironment()).toAppendStream(t, Row.class);
        stream.map(new FailingIdentityMapper<Row>(int32List.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        val runner = new Thread("runner") {
            @Override
            public void run() {
                try {
                    tableEnv.execute("read from earliest");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        runner.start();

        Thread.sleep(2000);
        assertEquals(StreamITCase.testResults(), int32List.subList(0, int32List.size() - 1));
    }

    @Test
    public void testTableSink() throws Exception {
        val tp = newTopic();
        val tableName = TopicName.get(tp).getLocalName();

        sendTypedMessages(tp, SchemaType.INT32, int32List, Optional.empty());

        val conf = getStreamingConfs();
        conf.put("$VAR_STARTING", "earliest");

        val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
        val tableEnv = context.createEnvironmentInstance().getTableEnvironment();

        tableEnv.useCatalog("pulsarcatalog1");

        val sinkDDL = "create table tableSink(v int)";
        val insertQ = "INSERT INTO tableSink SELECT * FROM " + tableName;

        tableEnv.sqlUpdate(sinkDDL);
        tableEnv.sqlUpdate(insertQ);

        val runner = new Thread("write to table") {
            @Override
            public void run() {
                try {
                    tableEnv.execute("write to table");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        runner.start();

        val conf1 = getStreamingConfs();
        conf1.put("$VAR_STARTING", "earliest");

        val context1 = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf1);
        val tableEnv1 = context.createEnvironmentInstance().getTableEnvironment();

        tableEnv1.useCatalog("pulsarcatalog1");

        val t = tableEnv1.scan("tableSink").select("value");
        val stream = ((StreamTableEnvironment) ((TableImpl) t).getTableEnvironment()).toAppendStream(t, Row.class);
        stream.map(new FailingIdentityMapper<Row>(int32List.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        val reader = new Thread("read") {
            @Override
            public void run() {
                try {
                    tableEnv.execute("read from earliest");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        reader.start();
        reader.join();
        assertEquals(StreamITCase.testResults(), int32List.subList(0, int32List.size() - 1));
    }

    @Test
    public void testSinkToExistingTopic() throws Exception {

        val tp = newTopic();
        val tableName = TopicName.get(tp).getLocalName();

        sendTypedMessages(tp, SchemaType.INT32, int32List, Optional.empty());
        sendTypedMessages("tableSink1", SchemaType.INT32, Arrays.asList(-1), Optional.empty());

        val conf = getStreamingConfs();
        conf.put("$VAR_STARTING", "earliest");

        val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf);
        val tableEnv = context.createEnvironmentInstance().getTableEnvironment();

        tableEnv.useCatalog("pulsarcatalog1");

        val insertQ = "INSERT INTO tableSink1 SELECT * FROM " + tableName;

        tableEnv.sqlUpdate(insertQ);

        val runner = new Thread("write to table") {
            @Override
            public void run() {
                try {
                    tableEnv.execute("write to table");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        runner.start();

        val conf1 = getStreamingConfs();
        conf1.put("$VAR_STARTING", "earliest");

        val context1 = createExecutionContext(CATALOGS_ENVIRONMENT_FILE_START, conf1);
        val tableEnv1 = context.createEnvironmentInstance().getTableEnvironment();

        tableEnv1.useCatalog("pulsarcatalog1");

        val t = tableEnv1.scan("tableSink1").select("value");
        val stream = ((StreamTableEnvironment) ((TableImpl) t).getTableEnvironment()).toAppendStream(t, Row.class);
        stream.map(new FailingIdentityMapper<Row>(int32List.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        val reader = new Thread("read") {
            @Override
            public void run() {
                try {
                    tableEnv.execute("read from earliest");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        reader.start();
        reader.join();
        assertEquals(StreamITCase.testResults(), int32List.subList(0, int32List.size() - 1));
    }

    private <T> ExecutionContext<T> createExecutionContext(String file, Map<String, String> replaceVars) throws Exception {
        final Environment env = EnvironmentFileUtil.parseModified(
            file,
            replaceVars);
        final Configuration flinkConfig = new Configuration();
        return new ExecutionContext<>(
            env,
            new SessionContext("test-session", new Environment()),
            Collections.emptyList(),
            flinkConfig,
            new Options(),
            Collections.singletonList(new DefaultCLI(flinkConfig)));
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

    private final AtomicInteger topicId = new AtomicInteger(0);

    private String newTopic() {
        return TopicName.get("topic-" + topicId.getAndIncrement()).toString();
    }
}
