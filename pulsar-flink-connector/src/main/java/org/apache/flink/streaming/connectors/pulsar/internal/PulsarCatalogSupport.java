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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC;

/**
 * catalog support.
 */
public class PulsarCatalogSupport {

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

    public TableSchema getTableSchema(ObjectPath tablePath) throws PulsarAdminException, IncompatibleSchemaException {
        String topicName = objectPath2TopicName(tablePath);
        final SchemaInfo pulsarSchema = pulsarMetadataReader.getPulsarSchema(topicName);
        return pulsarSchemaToTableSchema(pulsarSchema);
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
        pulsarMetadataReader.putSchema(topicName, tableSchemaToPulsarSchema(format, schema));
    }

    /**
     * Get default table properties by pulsar schemaRegistry.
     * @param objectPath
     * @return table properties
     * @throws IncompatibleSchemaException
     */
    public Map<String, String> getCatalogTableDefaultProperties(ObjectPath objectPath) throws IncompatibleSchemaException {
        String topicName = objectPath2TopicName(objectPath);
        final SchemaInfo pulsarSchema = pulsarMetadataReader.getPulsarSchema(topicName);
        Map<String, String> properties = schemaTranslator.schemaInfo2TableProperties(pulsarSchema);
        properties.put(TOPIC.key(), topicName);
        return properties;
    }

    private SchemaInfo tableSchemaToPulsarSchema(String format, TableSchema schema) throws IncompatibleSchemaException {
        // The exclusion logic for the key is not handled correctly here when the user sets the key-related fields using pulsar
        final DataType physicalRowDataType = schema.toPhysicalRowDataType();
        return SchemaUtils.tableSchemaToSchemaInfo(format, physicalRowDataType);
    }

    private TableSchema pulsarSchemaToTableSchema(SchemaInfo pulsarSchema) throws IncompatibleSchemaException {
        return schemaTranslator.pulsarSchemaToTableSchema(pulsarSchema);
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
