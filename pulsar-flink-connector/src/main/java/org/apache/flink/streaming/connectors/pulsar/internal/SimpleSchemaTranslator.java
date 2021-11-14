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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import com.google.protobuf.Descriptors;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableList;
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.SchemaBuilder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.EVENT_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.KEY_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.MESSAGE_ID_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PUBLISH_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;

/** flink 1.11 schema translator. */
public class SimpleSchemaTranslator extends SchemaTranslator {

    public static final List<DataTypes.Field> METADATA_FIELDS =
            ImmutableList.of(
                    DataTypes.FIELD(KEY_ATTRIBUTE_NAME, DataTypes.BYTES()),
                    DataTypes.FIELD(TOPIC_ATTRIBUTE_NAME, DataTypes.STRING()),
                    DataTypes.FIELD(MESSAGE_ID_NAME, DataTypes.BYTES()),
                    DataTypes.FIELD(PUBLISH_TIME_NAME, DataTypes.TIMESTAMP(3)),
                    DataTypes.FIELD(EVENT_TIME_NAME, DataTypes.TIMESTAMP(3)));

    private final boolean useExtendField;

    public SimpleSchemaTranslator() {
        this.useExtendField = false;
    }

    public SimpleSchemaTranslator(boolean useExtendField) {
        this.useExtendField = useExtendField;
    }

    @Override
    public SchemaInfo tableSchemaToPulsarSchema(TableSchema tableSchema)
            throws IncompatibleSchemaException {
        List<String> fieldsRemaining = new ArrayList<>(tableSchema.getFieldCount());
        for (String fieldName : tableSchema.getFieldNames()) {
            if (!PulsarOptions.META_FIELD_NAMES.contains(fieldName)) {
                fieldsRemaining.add(fieldName);
            }
        }

        DataType dataType;
        if (fieldsRemaining.size() == 1) {
            dataType = tableSchema.getFieldDataType(fieldsRemaining.get(0)).get();
        } else {
            List<DataTypes.Field> fieldList =
                    fieldsRemaining.stream()
                            .map(f -> DataTypes.FIELD(f, tableSchema.getFieldDataType(f).get()))
                            .collect(Collectors.toList());
            dataType = DataTypes.ROW(fieldList.toArray(new DataTypes.Field[0]));
        }
        return sqlType2PulsarSchema(dataType).getSchemaInfo();
    }

    public static org.apache.pulsar.client.api.Schema sqlType2PulsarSchema(DataType flinkType)
            throws IncompatibleSchemaException {
        if (flinkType instanceof AtomicDataType) {
            return atomicType2PulsarSchema(flinkType);
        } else if (flinkType instanceof FieldsDataType) {
            return avroSchema2PulsarSchema(sqlType2AvroSchema(flinkType));
        }
        throw new IncompatibleSchemaException(
                String.format("%s is not supported by Pulsar yet", flinkType.toString()), null);
    }

    static GenericSchema<GenericRecord> avroSchema2PulsarSchema(Schema avroSchema) {
        byte[] schemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
        SchemaInfoImpl si = new SchemaInfoImpl();
        si.setName("Avro");
        si.setSchema(schemaBytes);
        si.setType(SchemaType.AVRO);
        return org.apache.pulsar.client.api.Schema.generic(si);
    }

    public static Schema sqlType2AvroSchema(DataType flinkType) throws IncompatibleSchemaException {
        return sqlType2AvroSchema(flinkType, false, "record", "");
    }

    private static Schema sqlType2AvroSchema(
            DataType flinkType, boolean nullable, String recordName, String namespace)
            throws IncompatibleSchemaException {
        SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();
        LogicalTypeRoot type = flinkType.getLogicalType().getTypeRoot();
        Schema schema = null;

        if (flinkType instanceof AtomicDataType) {
            switch (type) {
                case BOOLEAN:
                    schema = builder.booleanType();
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    schema = builder.intType();
                    break;
                case BIGINT:
                    schema = builder.longType();
                    break;
                case DATE:
                    schema = LogicalTypes.date().addToSchema(builder.intType());
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    schema = LogicalTypes.timestampMicros().addToSchema(builder.longType());
                    break;
                case FLOAT:
                    schema = builder.floatType();
                    break;
                case DOUBLE:
                    schema = builder.doubleType();
                    break;
                case VARCHAR:
                    schema = builder.stringType();
                    break;
                case BINARY:
                case VARBINARY:
                    schema = builder.bytesType();
                    break;
                case DECIMAL:
                    DecimalType dt = (DecimalType) flinkType.getLogicalType();
                    LogicalTypes.Decimal avroType =
                            LogicalTypes.decimal(dt.getPrecision(), dt.getScale());
                    int fixedSize = minBytesForPrecision[dt.getPrecision()];
                    // Need to avoid naming conflict for the fixed fields
                    String name;
                    if (namespace.equals("")) {
                        name = recordName + ".fixed";
                    } else {
                        name = namespace + recordName + ".fixed";
                    }
                    schema = avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize));
                    break;
                default:
                    throw new IncompatibleSchemaException(
                            String.format("Unsupported type %s", flinkType.toString()), null);
            }
        } else if (flinkType instanceof CollectionDataType) {
            if (type == LogicalTypeRoot.ARRAY) {
                CollectionDataType cdt = (CollectionDataType) flinkType;
                DataType elementType = cdt.getElementDataType();
                schema =
                        builder.array()
                                .items(
                                        sqlType2AvroSchema(
                                                elementType,
                                                elementType.getLogicalType().isNullable(),
                                                recordName,
                                                namespace));
            } else {
                throw new IncompatibleSchemaException(
                        "Pulsar only support collection as array", null);
            }
        } else if (flinkType instanceof KeyValueDataType) {
            KeyValueDataType kvType = (KeyValueDataType) flinkType;
            DataType keyType = kvType.getKeyDataType();
            DataType valueType = kvType.getValueDataType();
            if (!(keyType instanceof AtomicDataType)
                    || keyType.getLogicalType().getTypeRoot() != LogicalTypeRoot.VARCHAR) {
                throw new IncompatibleSchemaException("Pulsar only support string key map", null);
            }
            schema =
                    builder.map()
                            .values(
                                    sqlType2AvroSchema(
                                            valueType,
                                            valueType.getLogicalType().isNullable(),
                                            recordName,
                                            namespace));
        } else if (flinkType instanceof FieldsDataType) {
            FieldsDataType fieldsDataType = (FieldsDataType) flinkType;
            String childNamespace =
                    namespace.equals("") ? recordName : namespace + "." + recordName;
            SchemaBuilder.FieldAssembler<Schema> fieldsAssembler =
                    builder.record(recordName).namespace(namespace).fields();
            RowType rowType = (RowType) fieldsDataType.getLogicalType();
            List<String> filedNames = rowType.getFieldNames();

            for (int i = 0; i < filedNames.size(); ++i) {
                String fieldName = filedNames.get(i);
                org.apache.flink.table.types.logical.LogicalType logicalType = rowType.getTypeAt(i);
                DataType ftype = TypeConversions.fromLogicalToDataType(logicalType);
                Schema fieldAvroSchema =
                        sqlType2AvroSchema(
                                ftype,
                                ftype.getLogicalType().isNullable(),
                                fieldName,
                                childNamespace);
                fieldsAssembler.name(fieldName).type(fieldAvroSchema).noDefault();
            }
            schema = fieldsAssembler.endRecord();
        } else {
            throw new IncompatibleSchemaException(
                    String.format("Unexpected type %s", flinkType.toString()), null);
        }

        if (nullable) {
            return Schema.createUnion(schema, NULL_SCHEMA);
        } else {
            return schema;
        }
    }

    public static SchemaInfo emptySchemaInfo() {
        return SchemaInfoImpl.builder()
                .name("empty")
                .type(SchemaType.NONE)
                .schema(new byte[0])
                .build();
    }

    private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

    private static int[] minBytesForPrecision = new int[39];

    static {
        for (int i = 0; i < minBytesForPrecision.length; i++) {
            minBytesForPrecision[i] = computeMinBytesForPrecision(i);
        }
    }

    private static int computeMinBytesForPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    @Override
    public TableSchema pulsarSchemaToTableSchema(SchemaInfo pulsarSchema)
            throws IncompatibleSchemaException {
        final FieldsDataType fieldsDataType = pulsarSchemaToFieldsDataType(pulsarSchema);
        RowType rt = (RowType) fieldsDataType.getLogicalType();
        List<DataType> fieldTypes = fieldsDataType.getChildren();
        return TableSchema.builder()
                .fields(
                        rt.getFieldNames().toArray(new String[0]),
                        fieldTypes.toArray(new DataType[0]))
                .build();
    }

    @Override
    public FieldsDataType pulsarSchemaToFieldsDataType(SchemaInfo schemaInfo)
            throws IncompatibleSchemaException {
        List<DataTypes.Field> mainSchema = new ArrayList<>();
        DataType dataType = schemaInfo2SqlType(schemaInfo);
        if (dataType instanceof FieldsDataType) {
            FieldsDataType fieldsDataType = (FieldsDataType) dataType;
            RowType rowType = (RowType) fieldsDataType.getLogicalType();
            List<String> fieldNames = rowType.getFieldNames();
            for (int i = 0; i < fieldNames.size(); i++) {
                org.apache.flink.table.types.logical.LogicalType logicalType = rowType.getTypeAt(i);
                DataTypes.Field field =
                        DataTypes.FIELD(
                                fieldNames.get(i),
                                TypeConversions.fromLogicalToDataType(logicalType));
                mainSchema.add(field);
            }

        } else {
            mainSchema.add(DataTypes.FIELD("value", dataType));
        }

        if (useExtendField) {
            mainSchema.addAll(METADATA_FIELDS);
        }
        return (FieldsDataType) DataTypes.ROW(mainSchema.toArray(new DataTypes.Field[0]));
    }

    @Override
    public DataType schemaInfo2SqlType(SchemaInfo si) throws IncompatibleSchemaException {
        switch (si.getType()) {
            case NONE:
            case BYTES:
                return DataTypes.BYTES();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case LOCAL_DATE:
                return DataTypes.DATE();
            case LOCAL_TIME:
                return DataTypes.TIME();
            case STRING:
                return DataTypes.STRING();
            case LOCAL_DATE_TIME:
                return DataTypes.TIMESTAMP(3);
            case INT8:
                return DataTypes.TINYINT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case FLOAT:
                return DataTypes.FLOAT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case INT16:
                return DataTypes.SMALLINT();
            case AVRO:
            case JSON:
                String avroSchemaString = new String(si.getSchema(), StandardCharsets.UTF_8);
                return AvroSchemaConverter.convertToDataType(avroSchemaString);
            case PROTOBUF_NATIVE:
                Descriptors.Descriptor descriptor =
                        ((GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(si))
                                .getProtobufNativeSchema();
                return proto2SqlType(descriptor);

            default:
                throw new UnsupportedOperationException(
                        String.format("We do not support %s currently.", si.getType()));
        }
    }

    public static DataType proto2SqlType(Descriptors.Descriptor descriptor)
            throws IncompatibleSchemaException {
        List<DataTypes.Field> fields = new ArrayList<>();
        List<Descriptors.FieldDescriptor> protoFields = descriptor.getFields();

        for (Descriptors.FieldDescriptor fieldDescriptor : protoFields) {
            DataType fieldType = proto2SqlType(fieldDescriptor);
            fields.add(DataTypes.FIELD(fieldDescriptor.getName(), fieldType));
        }

        if (fields.isEmpty()) {
            throw new IllegalArgumentException("No FieldDescriptors found");
        }
        return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
    }

    private static DataType proto2SqlType(Descriptors.FieldDescriptor field)
            throws IncompatibleSchemaException {
        Descriptors.FieldDescriptor.JavaType type = field.getJavaType();
        DataType dataType;
        switch (type) {
            case BOOLEAN:
                dataType = DataTypes.BOOLEAN();
                break;
            case BYTE_STRING:
                dataType = DataTypes.BYTES();
                break;
            case DOUBLE:
                dataType = DataTypes.DOUBLE();
                break;
            case ENUM:
                dataType = DataTypes.STRING();
                break;
            case FLOAT:
                dataType = DataTypes.FLOAT();
                break;
            case INT:
                dataType = DataTypes.INT();
                break;
            case LONG:
                dataType = DataTypes.BIGINT();
                break;
            case MESSAGE:
                Descriptors.Descriptor msg = field.getMessageType();
                if (field.isMapField()) {
                    // map
                    dataType =
                            DataTypes.MAP(
                                    proto2SqlType(msg.findFieldByName("key")),
                                    proto2SqlType(msg.findFieldByName("value")));
                } else {
                    // row
                    dataType = proto2SqlType(field.getMessageType());
                }
                break;
            case STRING:
                dataType = DataTypes.STRING();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown type: "
                                + type.toString()
                                + " for FieldDescriptor: "
                                + field.toString());
        }
        // list
        if (field.isRepeated() && !field.isMapField()) {
            dataType = DataTypes.ARRAY(dataType);
        }

        return dataType;
    }

    public boolean isUseExtendField() {
        return useExtendField;
    }
}
