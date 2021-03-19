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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.atomic.AtomicRowDataFormatFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.config.CatalogConfig.FLINK_PROPERTY_PREFIX;

/**
 * catalog support.
 */
public class PulsarCatalogSupport {

    private static final String COMMENT = "table.comment";
    private static final String IS_CATALOG_TOPIC = "is.catalog.topic";
    private PulsarMetadataReader pulsarMetadataReader;

    private SchemaTranslator schemaTranslator;

    public PulsarCatalogSupport(String adminUrl, ClientConfigurationData clientConfigurationData,
                                String subscriptionName,
                                Map<String, String> caseInsensitiveParams, int indexOfThisSubtask,
                                int numParallelSubtasks,
                                SchemaTranslator schemaTranslator) throws
            PulsarClientException {
        this.pulsarMetadataReader = new PulsarMetadataReader(
                adminUrl,
                clientConfigurationData,
                subscriptionName,
                caseInsensitiveParams,
                indexOfThisSubtask,
                numParallelSubtasks
        );
        this.schemaTranslator = schemaTranslator;
    }

    public List<String> listNamespaces() throws PulsarAdminException {
        return pulsarMetadataReader.listNamespaces();
    }

    public boolean namespaceExists(String databaseName) throws PulsarAdminException {
        return pulsarMetadataReader.namespaceExists(databaseName);
    }

    public void createNamespace(String name) throws PulsarAdminException {
        pulsarMetadataReader.createNamespace(name);
    }

    public List<String> getTopics(String databaseName) throws PulsarAdminException {
        return pulsarMetadataReader.getTopics(databaseName);
    }

    public CatalogTableImpl getTableSchema(ObjectPath tablePath,
                                           Map<String, String> properties)
            throws PulsarAdminException, IncompatibleSchemaException {
        String topicName = objectPath2TopicName(tablePath);
        final SchemaInfo pulsarSchema = pulsarMetadataReader.getPulsarSchema(topicName);
        return schemaToCatalogTable(pulsarSchema, tablePath, properties);
    }

    public boolean topicExists(ObjectPath tablePath) throws PulsarAdminException {
        String topicName = objectPath2TopicName(tablePath);
        return pulsarMetadataReader.topicExists(topicName);
    }

    public void createTopic(ObjectPath tablePath, int defaultNumPartitions, CatalogBaseTable table)
            throws PulsarAdminException,
            IncompatibleSchemaException {
        String topicName = objectPath2TopicName(tablePath);
        pulsarMetadataReader.createTopic(topicName, defaultNumPartitions);
    }

    public void putSchema(ObjectPath tablePath, CatalogBaseTable table, String format)
            throws PulsarAdminException, IncompatibleSchemaException {
        String topicName = objectPath2TopicName(tablePath);
        final TableSchema schema = table.getSchema();
        final SchemaInfo schemaInfo = tableSchemaToPulsarSchema(format, schema, table.getOptions());

        // Writing schemaInfo#properties causes the client to fail to consume it when it is a Pulsar native type.
        if (!StringUtils.equals(format, AtomicRowDataFormatFactory.IDENTIFIER)) {
            schemaInfo.setProperties(extractedProperties(table));
        }
        pulsarMetadataReader.putSchema(topicName, schemaInfo);
    }

    private Map<String, String> extractedProperties(CatalogBaseTable table) {
        DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
        tableSchemaProps.putTableSchema(Schema.SCHEMA, table.getSchema());
        if (table instanceof CatalogTable) {
            tableSchemaProps.putPartitionKeys(((CatalogTable) table).getPartitionKeys());
        }
        Map<String, String> properties = new HashMap<>(tableSchemaProps.asMap());
        properties = maskFlinkProperties(properties);
        if (table.getComment() == null) {
            properties.put(PulsarCatalogSupport.COMMENT, table.getComment());
        }
        properties.put(IS_CATALOG_TOPIC, "true");
        return properties;
    }

    public static Map<String, String> maskFlinkProperties(Map<String, String> properties) {
        return properties.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getValue() != null)
                .map(
                        e ->
                                new Tuple2<>(
                                        FLINK_PROPERTY_PREFIX + e.getKey(),
                                        e.getValue()))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    /**
     * Filter out Pulsar-created properties, and return Flink-created properties.
     * Note that 'is_generic' is a special key and this method will leave it as-is.
     */
    private static Map<String, String> retrieveFlinkProperties(Map<String, String> pulsarSchemaProperties) {
        return pulsarSchemaProperties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(FLINK_PROPERTY_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().replace(FLINK_PROPERTY_PREFIX, ""), e -> e.getValue()));
    }

    private SchemaInfo tableSchemaToPulsarSchema(String format, TableSchema schema,
                                                 Map<String, String> options) throws IncompatibleSchemaException {
        // The exclusion logic for the key is not handled correctly here when the user sets the key-related fields using pulsar
        final DataType physicalRowDataType = schema.toPhysicalRowDataType();
        return SchemaUtils.tableSchemaToSchemaInfo(format, physicalRowDataType, options);
    }

    private CatalogTableImpl schemaToCatalogTable(SchemaInfo pulsarSchema,
                                                  ObjectPath tablePath,
                                                  Map<String, String> flinkProperties)
            throws IncompatibleSchemaException {
        boolean isCatalogTopic = Boolean.parseBoolean(pulsarSchema.getProperties().get(IS_CATALOG_TOPIC));
        if (isCatalogTopic) {
            Map<String, String> properties = retrieveFlinkProperties(pulsarSchema.getProperties());
            DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
            tableSchemaProps.putProperties(properties);
            TableSchema tableSchema = tableSchemaProps.getOptionalTableSchema(Schema.SCHEMA)
                    .orElseGet(() -> tableSchemaProps.getOptionalTableSchema("generic.table.schema")
                            .orElseThrow(() -> new CatalogException(
                                    "Failed to get table schema from properties for generic table " + tablePath)));
            List<String> partitionKeys = tableSchemaProps.getPartitionKeys();
            // remove the schema from properties
            properties = CatalogTableImpl.removeRedundant(properties, tableSchema, partitionKeys);
            properties.putAll(flinkProperties);
            properties.remove(IS_CATALOG_TOPIC);
            String comment = properties.remove(PulsarCatalogSupport.COMMENT);
            return new CatalogTableImpl(tableSchema, partitionKeys, properties, comment);
        } else {
            final TableSchema tableSchema = schemaTranslator.pulsarSchemaToTableSchema(pulsarSchema);
            return new CatalogTableImpl(tableSchema, flinkProperties, "");
        }
    }

    public static String objectPath2TopicName(ObjectPath objectPath) {
        NamespaceName ns = NamespaceName.get(objectPath.getDatabaseName());
        String topic = objectPath.getObjectName();
        TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, topic);
        return fullName.toString();
    }

    public void close() {
        if (pulsarMetadataReader != null) {
            pulsarMetadataReader.close();
        }
    }

    public void deleteTopic(ObjectPath tablePath) throws PulsarAdminException {
        String topicName = objectPath2TopicName(tablePath);
        pulsarMetadataReader.deleteTopic(topicName);
    }

    public void deleteNamespace(String name) throws PulsarAdminException {
        pulsarMetadataReader.deleteNamespace(name);
    }
}
