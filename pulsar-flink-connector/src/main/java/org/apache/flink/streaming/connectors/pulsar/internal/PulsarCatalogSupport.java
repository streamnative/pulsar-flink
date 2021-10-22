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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.pulsar.TableSchemaHelper;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PROPERTIES_PREFIX;

/**
 * catalog support.
 */
public class PulsarCatalogSupport {

    private static final String FLINK_CATALOG_TENANT = "__flink_catalog";

    private static final String TABLE_PREFIX = "table_";

    private static final String TABLE_COMMENT = "table.comment";
    private static final String IS_CATALOG_TOPIC = "is.catalog.topic";

    private final Map<String, String> properties;

    private final PulsarMetadataReader pulsarMetadataReader;

    private SchemaTranslator schemaTranslator;

    private final ClientConfigurationData clientConf;

    public PulsarCatalogSupport(String adminUrl,
                                Map<String, String> properties,
                                String subscriptionName,
                                Map<String, String> caseInsensitiveParams, int indexOfThisSubtask,
                                int numParallelSubtasks,
                                SchemaTranslator schemaTranslator) throws
        PulsarClientException, PulsarAdminException {
        this.properties = properties;

        this.clientConf = new ClientConfigurationData();
        clientConf.setAuthParams(properties.get(PROPERTIES_PREFIX + PulsarOptions.AUTH_PARAMS_KEY));
        clientConf.setAuthPluginClassName(
            properties.get(PROPERTIES_PREFIX + PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY));

        this.pulsarMetadataReader = new PulsarMetadataReader(
            adminUrl,
            clientConf,
            subscriptionName,
            caseInsensitiveParams,
            indexOfThisSubtask,
            numParallelSubtasks
        );
        this.schemaTranslator = schemaTranslator;

        // Initialize the dedicated tenant if necessary
        if (!pulsarMetadataReader.tenantExists(FLINK_CATALOG_TENANT)) {
            pulsarMetadataReader.createTenant(FLINK_CATALOG_TENANT);
        }
    }

    @VisibleForTesting
    protected PulsarCatalogSupport(PulsarMetadataReader metadataReader,
                                SchemaTranslator schemaTranslator) {
        this.pulsarMetadataReader = metadataReader;
        this.schemaTranslator = schemaTranslator;

        // TODO: initialize the value
        this.properties = new HashMap<>();
        this.clientConf = new ClientConfigurationData();
    }

    public static boolean isNativeFlinkDatabase(String name) {
        return !name.contains("/");
    }

    public List<String> listDatabases() throws PulsarAdminException {
        List<String> databases = new ArrayList<>();
        for (String ns : pulsarMetadataReader.listNamespaces()) {
            if (ns.startsWith(FLINK_CATALOG_TENANT)) {
                // native flink database
                databases.add(ns.substring(FLINK_CATALOG_TENANT.length() + 1));
            } else {
                // pulsar tenant/namespace mapped database
                databases.add(ns);
            }
        }
        return databases;
    }

    public boolean databaseExists(String name) throws PulsarAdminException {
        if (isNativeFlinkDatabase(name)) {
            return pulsarMetadataReader.namespaceExists(FLINK_CATALOG_TENANT + "/" + name);
        } else {
            return pulsarMetadataReader.namespaceExists(name);
        }
    }

    public void createDatabase(String name) throws PulsarAdminException {
        pulsarMetadataReader.createNamespace(FLINK_CATALOG_TENANT + "/" + name);
    }

    public List<String> listTables(String database) throws PulsarAdminException {
        if (isNativeFlinkDatabase(database)) {
            List<String> tables = new ArrayList<>();
            List<String> topics = pulsarMetadataReader.getTopics(FLINK_CATALOG_TENANT + "/" + database);
            for (String topic : topics) {
                tables.add(topic.substring(TABLE_PREFIX.length()));
            }
            return tables;
        } else {
            return pulsarMetadataReader.getTopics(database);
        }
    }

    public boolean tableExists(ObjectPath tablePath) throws PulsarAdminException {
        String topicName = objectNameToTopicName(tablePath);
        return pulsarMetadataReader.topicExists(topicName);
    }

    public CatalogTable getTable(ObjectPath tablePath)
        throws PulsarAdminException, IncompatibleSchemaException {
        String topicName = objectNameToTopicName(tablePath);
        if (isNativeFlinkDatabase(tablePath.getDatabaseName())) {
            final SchemaInfo metadataSchema = pulsarMetadataReader.getPulsarSchema(topicName);
            try {
                return TableSchemaHelper.deserialize(metadataSchema, generateDefaultTableOptions());
            } catch (Exception e) {
                e.printStackTrace();
                throw new CatalogException("Failed to fetch metadata for flink table: " + tablePath.getObjectName());
            }
        } else {
            final SchemaInfo pulsarSchema = pulsarMetadataReader.getPulsarSchema(topicName);
            return schemaToCatalogTable(pulsarSchema, tablePath, properties);
        }
    }

    private Map<String, String> generateDefaultTableOptions() {
        // TODO refine all options needed to pass as table default
        Map<String, String> defaultTableOptions = new HashMap<>();
        defaultTableOptions.put("type", properties.get("type"));
        defaultTableOptions.put(PulsarOptions.SERVICE_URL_OPTION_KEY, properties.get(PulsarOptions.SERVICE_URL_OPTION_KEY));
        defaultTableOptions.put(PulsarOptions.ADMIN_URL_OPTION_KEY, properties.get(PulsarOptions.ADMIN_URL_OPTION_KEY));

        if (properties.get(FactoryUtil.FORMAT.key()) != null) {
            defaultTableOptions.put(FactoryUtil.FORMAT.key(), properties.get(FactoryUtil.FORMAT.key()));
        }

        // move all configs with properties prefix to default table config
        properties.forEach((key, value) -> {
            if (key.startsWith(PROPERTIES_PREFIX)) {
                defaultTableOptions.put(key, value);
            }});

        return defaultTableOptions;
    }

    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws PulsarAdminException {
        if (isNativeFlinkDatabase(tablePath.getDatabaseName())) {
            String topicName = objectNameToTopicName(tablePath);
            // manually clean the schema to avoid affecting new table with same name use old schema
            pulsarMetadataReader.deleteSchema(topicName);
            pulsarMetadataReader.deleteTopic(topicName);
        } else {
            throw new CatalogException("Can't delete normal pulsar topic");
        }
    }

    public void createTable(ObjectPath tablePath, CatalogBaseTable table)
        throws PulsarAdminException, IncompatibleSchemaException {
        // TODO validate the passed options

        // only allow creating table in flink database, the topic is used to host table information
        if (!isNativeFlinkDatabase(tablePath.getDatabaseName())) {
            throw new CatalogException(String.format(
                "Can't create flink table under pulsar tenant/namespace: %s", tablePath.getDatabaseName()));
        }

        String topicName = objectNameToTopicName(tablePath);
        pulsarMetadataReader.createTopic(topicName, 0);

        // use pulsar schema to store flink table information
        try {
            pulsarMetadataReader.uploadSchema(topicName, TableSchemaHelper.serialize(table));
        } catch  (Exception e) {
            // delete topic if table info cannot be persisted
            try {
                pulsarMetadataReader.deleteTopic(topicName);
            } catch (PulsarAdminException ex) {
                // do nothing
            }
            e.printStackTrace();
            throw new CatalogException("Can't store table metadata");
        }
    }

    private CatalogTable schemaToCatalogTable(SchemaInfo pulsarSchema,
                                              ObjectPath tablePath,
                                              Map<String, String> flinkProperties)
        throws IncompatibleSchemaException {
        boolean isCatalogTopic = Boolean.parseBoolean(pulsarSchema.getProperties().get(IS_CATALOG_TOPIC));
        if (isCatalogTopic) {
            Map<String, String> properties = new HashMap<>();
            DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
            tableSchemaProps.putProperties(properties);
            TableSchema tableSchema = tableSchemaProps.getOptionalTableSchema(org.apache.flink.table.descriptors.Schema.SCHEMA)
                .orElseGet(() -> tableSchemaProps.getOptionalTableSchema("generic.table.schema")
                    .orElseThrow(() -> new CatalogException(
                        "Failed to get table schema from properties for generic table " + tablePath)));
            List<String> partitionKeys = tableSchemaProps.getPartitionKeys();
            // remove the schema from properties
            properties = CatalogTableImpl.removeRedundant(properties, tableSchema, partitionKeys);
            properties.putAll(flinkProperties);
            properties.remove(IS_CATALOG_TOPIC);
            String comment = properties.remove(PulsarCatalogSupport.TABLE_COMMENT);
            return new CatalogTableImpl(tableSchema, properties, comment);
        } else {
            final TableSchema tableSchema = schemaTranslator.pulsarSchemaToTableSchema(pulsarSchema);
            return new CatalogTableImpl(tableSchema, flinkProperties, "");
        }
    }

    private String objectNameToTopicName(ObjectPath objectPath) {
        String database;
        String topic;

        if (isNativeFlinkDatabase(objectPath.getDatabaseName())) {
            database = FLINK_CATALOG_TENANT + "/" + objectPath.getDatabaseName();
            topic = TABLE_PREFIX + objectPath.getObjectName();
        } else {
            database = objectPath.getDatabaseName();
            topic = objectPath.getObjectName();
        }

        NamespaceName ns = NamespaceName.get(database);
        TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, topic);

        return fullName.toString();
    }

    public void close() {
        if (pulsarMetadataReader != null) {
            pulsarMetadataReader.close();
        }
    }

    public void deleteNamespace(String name) throws PulsarAdminException {
        pulsarMetadataReader.deleteNamespace(name);
    }
}
