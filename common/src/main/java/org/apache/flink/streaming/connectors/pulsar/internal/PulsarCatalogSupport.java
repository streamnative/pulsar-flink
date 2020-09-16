package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import java.util.List;
import java.util.Map;

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

    public void putSchema(ObjectPath tablePath, CatalogBaseTable table)
            throws PulsarAdminException, IncompatibleSchemaException {
        String topicName = objectPath2TopicName(tablePath);
        final TableSchema schema = table.getSchema();
        pulsarMetadataReader.putSchema(topicName, tableSchemaToPulsarSchema(schema));
    }

    // TODO 补充 schema的转换
    private SchemaInfo tableSchemaToPulsarSchema(TableSchema schema) throws IncompatibleSchemaException {
        return schemaTranslator.tableSchemaToPulsarSchema(schema);
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
