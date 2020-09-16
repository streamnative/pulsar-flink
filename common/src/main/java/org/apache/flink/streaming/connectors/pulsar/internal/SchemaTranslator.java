package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.pulsar.common.schema.SchemaInfo;
import java.io.Serializable;

public interface SchemaTranslator extends Serializable {
    // flink 1.11 初始化  Boolean.parseBoolean(properties.getProperty(USE_EXTEND_FIELD))字段
    // TODO 补充 schema的转换
    SchemaInfo tableSchemaToPulsarSchema(TableSchema schema) throws IncompatibleSchemaException;

    TableSchema pulsarSchemaToTableSchema(SchemaInfo pulsarSchema) throws IncompatibleSchemaException;

    FieldsDataType pulsarSchemaToFieldsDataType(SchemaInfo pulsarSchema) throws IncompatibleSchemaException;

    DataType schemaInfo2SqlType(SchemaInfo si) throws IncompatibleSchemaException;
}
