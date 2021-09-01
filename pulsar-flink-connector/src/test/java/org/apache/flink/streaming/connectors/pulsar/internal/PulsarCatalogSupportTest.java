package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * unit test for PulsarCatalogSupport.
 */
public class PulsarCatalogSupportTest {

    @Test
    public void putSchema() throws PulsarAdminException {
        final CatalogBaseTable catalogBaseTable = mock(CatalogBaseTable.class);
        final ResolvedSchema schema2 = ResolvedSchema.of(
                Column.physical("physical_1", DataTypes.STRING()),
                Column.physical("physical_2", DataTypes.INT()),
                Column.metadata("eventTime", DataTypes.TIMESTAMP(3), "eventTime", false),
                Column.metadata("properties", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()), "properties",
                        false),
                Column.metadata("topic", DataTypes.STRING(), "topic", true),
                Column.metadata("sequenceId", DataTypes.BIGINT(), "sequenceId", true),
                Column.physical("key", DataTypes.STRING()),
                Column.physical("physical_3", DataTypes.BOOLEAN())
        );
        when(catalogBaseTable.getSchema()).thenReturn(TableSchema.fromResolvedSchema(schema2));
        when(catalogBaseTable.getUnresolvedSchema()).thenReturn(
                Schema.newBuilder().fromResolvedSchema(schema2).build());
        final Configuration configuration = getCatalogConfig();

        PulsarMetadataReader metadataReader = verify(mock(PulsarMetadataReader.class), data -> {
            final SchemaInfo schemaInfo = data.getTarget().getInvocation().getArgument(1, SchemaInfo.class);
            final org.apache.avro.Schema schema =
                    new org.apache.avro.Schema.Parser().parse(new String(schemaInfo.getSchema()));

            Assert.assertEquals(schema.getFields().size(), 3);
            Assert.assertEquals(schema.getFields().get(0).name(), "physical_1");
            Assert.assertEquals(schema.getFields().get(1).name(), "physical_2");
            Assert.assertEquals(schema.getFields().get(2).name(), "physical_3");
        });
        final PulsarCatalogSupport catalogSupport =
                new PulsarCatalogSupport(metadataReader, new SimpleSchemaTranslator());
        catalogSupport.putSchema(ObjectPath.fromString("public/default.test"), catalogBaseTable, configuration);
    }

    private Configuration getCatalogConfig() {
        final Configuration configuration = new Configuration();
        configuration.set(PulsarTableOptions.VALUE_FORMAT, "avro");
        configuration.set(PulsarTableOptions.KEY_FORMAT, "string");
        configuration.set(PulsarTableOptions.KEY_FIELDS, Collections.singletonList("key"));
        configuration.set(PulsarTableOptions.VALUE_FIELDS_INCLUDE, PulsarTableOptions.ValueFieldsStrategy.EXCEPT_KEY);
        return configuration;
    }
}
