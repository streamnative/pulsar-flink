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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions;

import java.util.Collections;

/**
 * unit test for PulsarCatalogSupport.
 */
public class PulsarCatalogSupportTest {

//    @Test
//    public void putSchema() throws PulsarAdminException {
//        final CatalogBaseTable catalogBaseTable = mock(CatalogBaseTable.class);
//
//        final TableSchema tableSchema = TableSchema.builder()
//                .add(TableColumn.physical("physical_1", DataTypes.STRING()))
//                .add(TableColumn.physical("physical_2", DataTypes.INT()))
//                .add(TableColumn.metadata("eventTime", DataTypes.TIMESTAMP(3), "eventTime", false))
//                .add(TableColumn.metadata("properties", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
//                        "properties",
//                        false))
//                .add(TableColumn.metadata("topic", DataTypes.STRING(), "topic", true))
//                .add(TableColumn.metadata("sequenceId", DataTypes.BIGINT(), "sequenceId", true))
//                .add(TableColumn.physical("key", DataTypes.STRING()))
//                .add(TableColumn.physical("physical_3", DataTypes.BOOLEAN()))
//                .build();
//        when(catalogBaseTable.getSchema()).thenReturn(tableSchema);
//        final Configuration configuration = getCatalogConfig();
//
//        PulsarMetadataReader metadataReader = verify(mock(PulsarMetadataReader.class), data -> {
//            final SchemaInfo schemaInfo = data.getTarget().getInvocation().getArgument(1, SchemaInfo.class);
//            final org.apache.avro.Schema schema =
//                    new org.apache.avro.Schema.Parser().parse(new String(schemaInfo.getSchema()));
//
//            Assert.assertEquals(schema.getFields().size(), 3);
//            Assert.assertEquals(schema.getFields().get(0).name(), "physical_1");
//            Assert.assertEquals(schema.getFields().get(1).name(), "physical_2");
//            Assert.assertEquals(schema.getFields().get(2).name(), "physical_3");
//        });
//        final PulsarCatalogSupport catalogSupport =
//                new PulsarCatalogSupport(metadataReader, new SimpleSchemaTranslator());
//        catalogSupport.putSchema(ObjectPath.fromString("public/default.test"), catalogBaseTable, configuration);
//    }

    private Configuration getCatalogConfig() {
        final Configuration configuration = new Configuration();
        configuration.set(PulsarTableOptions.VALUE_FORMAT, "avro");
        configuration.set(PulsarTableOptions.KEY_FORMAT, "string");
        configuration.set(PulsarTableOptions.KEY_FIELDS, Collections.singletonList("key"));
        configuration.set(PulsarTableOptions.VALUE_FIELDS_INCLUDE, PulsarTableOptions.ValueFieldsStrategy.EXCEPT_KEY);
        return configuration;
    }
}
