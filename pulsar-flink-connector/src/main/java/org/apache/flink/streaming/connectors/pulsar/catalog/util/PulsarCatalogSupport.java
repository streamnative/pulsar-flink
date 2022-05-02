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

package org.apache.flink.streaming.connectors.pulsar.catalog.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarDynamicTableFactory;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** catalog support. */
public class PulsarCatalogSupport {

    private static final String TABLE_PREFIX = "table_";

    private final PulsarMetadataReader pulsarMetadataReader;

    private final String flinkCatalogTenant;

    private SchemaTranslator schemaTranslator;

    public PulsarCatalogSupport(
            String adminUrl,
            ClientConfigurationData clientConf,
            String flinkTenant,
            SchemaTranslator schemaTranslator)
            throws PulsarClientException, PulsarAdminException {
        this.pulsarMetadataReader =
                new PulsarMetadataReader(adminUrl, clientConf, "", new HashMap<>(), -1, -1);
        this.schemaTranslator = schemaTranslator;
        this.flinkCatalogTenant = flinkTenant;

        // Initialize the dedicated tenant if necessary
        if (!pulsarMetadataReader.tenantExists(flinkCatalogTenant)) {
            pulsarMetadataReader.createTenant(flinkCatalogTenant);
        }
    }

    @VisibleForTesting
    public PulsarCatalogSupport(
            PulsarMetadataReader metadataReader,
            SchemaTranslator schemaTranslator,
            String flinkCatalogTenant) {
        this.pulsarMetadataReader = metadataReader;
        this.schemaTranslator = schemaTranslator;
        this.flinkCatalogTenant = flinkCatalogTenant;
    }

    /**
     * A generic database stored in pulsar catalog should consist of alphanumeric characters. A
     * pulsar tenant/namespace mapped database should contain the "/" in between tenant and
     * namespace
     *
     * @param name the database name
     * @return false if the name contains "/", which indicate it's a pulsar tenant/namespace mapped
     *     database
     */
    private boolean isGenericDatabase(String name) {
        return !name.contains("/");
    }

    private String completeGenericDatabasePath(String name) {
        return this.flinkCatalogTenant + "/" + name;
    }

    public List<String> listDatabases() throws PulsarAdminException {
        List<String> databases = new ArrayList<>();
        for (String ns : pulsarMetadataReader.listNamespaces()) {
            if (ns.startsWith(flinkCatalogTenant)) {
                // generic database
                databases.add(ns.substring(flinkCatalogTenant.length() + 1));
            } else {
                // pulsar tenant/namespace mapped database
                databases.add(ns);
            }
        }
        return databases;
    }

    public boolean databaseExists(String name) throws PulsarAdminException {
        if (isGenericDatabase(name)) {
            return pulsarMetadataReader.namespaceExists(completeGenericDatabasePath(name));
        } else {
            return pulsarMetadataReader.namespaceExists(name);
        }
    }

    public void createDatabase(String name) throws PulsarAdminException {
        if (isGenericDatabase(name)) {
            pulsarMetadataReader.createNamespace(completeGenericDatabasePath(name));
        } else {
            throw new CatalogException("Can't create pulsar tenant/namespace mapped database");
        }
    }

    public void dropDatabase(String name) throws PulsarAdminException {
        if (isGenericDatabase(name)) {
            pulsarMetadataReader.deleteNamespace(completeGenericDatabasePath(name));
        } else {
            throw new CatalogException("Can't drop pulsar tenant/namespace mapped database");
        }
    }

    public List<String> listTables(String name) throws PulsarAdminException {
        if (isGenericDatabase(name)) {
            List<String> tables = new ArrayList<>();
            List<String> topics = pulsarMetadataReader.getTopics(completeGenericDatabasePath(name));
            for (String topic : topics) {
                tables.add(topic.substring(TABLE_PREFIX.length()));
            }
            return tables;
        } else {
            return pulsarMetadataReader.getTopics(name);
        }
    }

    public boolean tableExists(ObjectPath tablePath) throws PulsarAdminException {
        String topicName = objectNameToTopicName(tablePath);
        return pulsarMetadataReader.topicExists(topicName);
    }

    public CatalogTable getTable(ObjectPath tablePath) throws PulsarAdminException {
        String topicName = objectNameToTopicName(tablePath);
        if (isGenericDatabase(tablePath.getDatabaseName())) {
            try {
                final SchemaInfo metadataSchema = pulsarMetadataReader.getPulsarSchema(topicName);
                Map<String, String> tableProperties =
                        TableSchemaHelper.generateTableProperties(metadataSchema);
                CatalogTable table = CatalogTable.fromProperties(tableProperties);
                table.getOptions().put(PulsarOptions.GENERIC, Boolean.TRUE.toString());
                return CatalogTable.of(
                        table.getUnresolvedSchema(),
                        table.getComment(),
                        table.getPartitionKeys(),
                        enrichTableOptions(table.getOptions()));
            } catch (Exception e) {
                e.printStackTrace();
                throw new CatalogException(
                        "Failed to fetch metadata for generic table: " + tablePath.getObjectName());
            }
        } else {
            final SchemaInfo pulsarSchema = pulsarMetadataReader.getPulsarSchema(topicName);
            return schemaToCatalogTable(pulsarSchema);
        }
    }

    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws PulsarAdminException {
        if (isGenericDatabase(tablePath.getDatabaseName())) {
            String topicName = objectNameToTopicName(tablePath);
            // manually clean the schema to avoid affecting new table with same name use old schema
            pulsarMetadataReader.deleteSchema(topicName);
            pulsarMetadataReader.deleteTopic(topicName);
        } else {
            throw new CatalogException("Can't delete normal pulsar topic");
        }
    }

    public void createTable(ObjectPath tablePath, ResolvedCatalogTable table)
            throws PulsarAdminException {
        // only allow creating table in generic database, the topic is used to save table
        // information
        if (!isGenericDatabase(tablePath.getDatabaseName())) {
            throw new CatalogException(
                    String.format(
                            "Can't create generic table under pulsar tenant/namespace: %s",
                            tablePath.getDatabaseName()));
        }

        String topicName = objectNameToTopicName(tablePath);
        pulsarMetadataReader.createTopic(topicName, 0);

        // use pulsar schema to store generic table information
        try {
            SchemaInfo schemaInfo = TableSchemaHelper.generateSchemaInfo(table.toProperties());
            pulsarMetadataReader.uploadSchema(topicName, schemaInfo);
        } catch (Exception e) {
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

    private CatalogTable schemaToCatalogTable(SchemaInfo pulsarSchema) {
        final TableSchema tableSchema = schemaTranslator.pulsarSchemaToTableSchema(pulsarSchema);
        final Schema schema =
                Schema.newBuilder().fromRowDataType(tableSchema.toRowDataType()).build();
        return CatalogTable.of(schema, "", Collections.emptyList(), enrichTableOptions(null));
    }

    // enrich table properties with proper catalog configs
    private Map<String, String> enrichTableOptions(final Map<String, String> tableOptions) {
        // TODO (nlu): add necessary table options
        Map<String, String> enrichedTableOptions = new HashMap<>();
        enrichedTableOptions.put(FactoryUtil.CONNECTOR.key(), PulsarDynamicTableFactory.IDENTIFIER);
        enrichedTableOptions.put(
                PulsarOptions.ADMIN_URL_OPTION_KEY, pulsarMetadataReader.getAdminUrl());
        enrichedTableOptions.put(
                PulsarOptions.SERVICE_URL_OPTION_KEY,
                pulsarMetadataReader.getClientConf().getServiceUrl());

        String authPlugin = pulsarMetadataReader.getClientConf().getAuthPluginClassName();
        if (authPlugin != null && !authPlugin.isEmpty()) {
            enrichedTableOptions.put(
                    PulsarTableOptions.PROPERTIES_PREFIX + PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY,
                    authPlugin);
        }

        String authParams = pulsarMetadataReader.getClientConf().getAuthParams();
        if (authParams != null && !authParams.isEmpty()) {
            enrichedTableOptions.put(
                    PulsarTableOptions.PROPERTIES_PREFIX + PulsarOptions.AUTH_PARAMS_KEY,
                    authParams);
        }

        String tlsTrustCertFilePath =
                pulsarMetadataReader.getClientConf().getTlsTrustCertsFilePath();
        if (tlsTrustCertFilePath != null && !tlsTrustCertFilePath.isEmpty()) {
            enrichedTableOptions.put(
                    PulsarTableOptions.PROPERTIES_PREFIX + PulsarOptions.TLS_TRUSTCERTS_FILEPATH,
                    tlsTrustCertFilePath);
        }

        if (tableOptions != null) {
            // table options could overwrite the default options provided above
            enrichedTableOptions.putAll(tableOptions);
        }

        if (!enrichedTableOptions.containsKey(PulsarTableOptions.VALUE_FORMAT.key())
                && !enrichedTableOptions.containsKey(FactoryUtil.FORMAT.key())) {
            enrichedTableOptions.put(FactoryUtil.FORMAT.key(), "raw");
        }

        return enrichedTableOptions;
    }

    private String objectNameToTopicName(ObjectPath objectPath) {
        String database;
        String topic;

        if (isGenericDatabase(objectPath.getDatabaseName())) {
            database = flinkCatalogTenant + "/" + objectPath.getDatabaseName();
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
