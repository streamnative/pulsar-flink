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
package org.apache.flink.streaming.connectors.pulsar.internal;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.shade.org.apache.avro.Conversions;
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;
import org.apache.pulsar.shade.org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.ARRAY;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.BYTES;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.ENUM;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.FIXED;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.MAP;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.RECORD;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.STRING;

@Slf4j
public class PulsarSerializer {

    private final DataType flinkType;

    private final boolean nullable;

    @Getter(lazy = true)
    private final Conversions.DecimalConversion decimalConversion =
        new Conversions.DecimalConversion();

    private final Schema rootAvroType;

    private final Function<Object, Object> converter;

    public PulsarSerializer(DataType flinkType, boolean nullable) {
        this.flinkType = flinkType;
        this.nullable = nullable;

        try {
            this.rootAvroType = SchemaUtils.sqlType2AvroSchema(flinkType);

            val actualAvroType = resolveNullableType(rootAvroType, nullable);
            Function<Object, Object> baseConverter;
            if (flinkType instanceof FieldsDataType) {
                val st = (FieldsDataType) flinkType;
                baseConverter = newStructConverter(st, actualAvroType);
            } else {
                BiFunction<PositionedGetter, Integer, Object> converter = singleValueConverter(flinkType, actualAvroType);
                baseConverter =
                    data -> converter.apply(new PositionedGetter((Row) data), 0);
            }

            if (nullable) {
                this.converter = data -> {
                    if (data == null) {
                        return null;
                    } else {
                        return baseConverter.apply(data);
                    }
                };
            } else {
                this.converter = baseConverter;
            }

        } catch (SchemaUtils.IncompatibleSchemaException e) {
            log.error("Failed to create serializer while converting flink type to avro type");
            throw new RuntimeException(e);
        }
    }

    public Object serialize(Object flinkData) {
        return converter.apply(flinkData);
    }

    private BiFunction<PositionedGetter, Integer, Object> singleValueConverter(DataType dataType, Schema avroType) throws SchemaUtils.IncompatibleSchemaException {
        val tpe = dataType.getLogicalType().getTypeRoot();
        val atpe = avroType.getType();
        if (tpe == LogicalTypeRoot.NULL && atpe == Schema.Type.NULL) {
            return (getter, ordinal) -> null;
        } else if ((tpe == LogicalTypeRoot.BOOLEAN && atpe == Schema.Type.BOOLEAN) ||
            (tpe == LogicalTypeRoot.TINYINT && atpe == Schema.Type.INT) ||
            (tpe == LogicalTypeRoot.SMALLINT && atpe == Schema.Type.INT) ||
            (tpe == LogicalTypeRoot.INTEGER && atpe == Schema.Type.INT) ||
            (tpe == LogicalTypeRoot.BIGINT && atpe == Schema.Type.LONG) ||
            (tpe == LogicalTypeRoot.FLOAT && atpe == Schema.Type.FLOAT) ||
            (tpe == LogicalTypeRoot.DOUBLE && atpe == Schema.Type.DOUBLE) ||
            (tpe == LogicalTypeRoot.VARCHAR && atpe == Schema.Type.STRING) ||
            (tpe == LogicalTypeRoot.VARBINARY && atpe == Schema.Type.BYTES) ||
            (tpe == LogicalTypeRoot.DATE && atpe == Schema.Type.INT)) {
            return (getter, ordinal) -> getter.getField(ordinal);
        } else if (tpe == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE && atpe == Schema.Type.LONG) {
            val altpe = avroType.getLogicalType();
            if (altpe instanceof LogicalTypes.TimestampMillis || altpe instanceof LogicalTypes.TimestampMicros) {
                return (getter, ordinal) -> getter.getField(ordinal);
            } else {
                throw new SchemaUtils.IncompatibleSchemaException(
                    "Cannot convert flink timestamp to avro logical type " + altpe.toString());
            }
        } else {
            throw new SchemaUtils.IncompatibleSchemaException(String.format(
                "Cannot convert flink type %s to avro type %s", dataType.toString(), avroType.toString(true)));
        }
    }

    private Function<Object, Object> newStructConverter(FieldsDataType dataType, Schema avroStruct) throws SchemaUtils.IncompatibleSchemaException {
        if (avroStruct.getType() != RECORD ||
            avroStruct.getFields().size() != dataType.getFieldDataTypes().size()) {
            throw new SchemaUtils.IncompatibleSchemaException(
                String.format("Cannot convert Flink type %s to Avro type %s.", dataType.toString(), avroStruct.toString(true)));
        }

        val fieldsType = dataType.getFieldDataTypes();
        val fields = ((RowType) dataType.getLogicalType()).getFields();

        List<BiFunction<PositionedGetter, Integer, Object>> fieldConverters = new ArrayList<>();

        for (int i = 0; i < fields.size(); i++) {
            val rf = fields.get(i);
            val dt = fieldsType.get(rf.getName());
            val at = avroStruct.getFields().get(i);
            fieldConverters.add(newConverter(dt, resolveNullableType(at.schema(), dt.getLogicalType().isNullable())));
        }
        val numFields = fieldsType.size();

        return row -> {
            val pSchema = SchemaUtils.avroSchema2PulsarSchema(avroStruct);
            val builder = pSchema.newRecordBuilder();
            val rowX = (Row) row;

            for (int i = 0; i < numFields; i++) {
                if (rowX.getField(i) == null) {
                    builder.set(pSchema.getFields().get(i), null);
                } else {
                    builder.set(pSchema.getFields().get(i), fieldConverters.get(i).apply(new PositionedGetter(rowX), i));
                }
            }
            return (GenericAvroRecord) builder.build();
        };

    }

    private BiFunction<PositionedGetter, Integer, Object> newConverter(DataType dataType, Schema avroType) throws SchemaUtils.IncompatibleSchemaException {
        val tpe = dataType.getLogicalType().getTypeRoot();
        val atpe = avroType.getType();
        if (tpe == LogicalTypeRoot.NULL && atpe == Schema.Type.NULL) {
            return (getter, ordinal) -> null;
        } else if ((tpe == LogicalTypeRoot.BOOLEAN && atpe == Schema.Type.BOOLEAN) ||
            (tpe == LogicalTypeRoot.TINYINT && atpe == Schema.Type.INT) ||
            (tpe == LogicalTypeRoot.SMALLINT && atpe == Schema.Type.INT) ||
            (tpe == LogicalTypeRoot.INTEGER && atpe == Schema.Type.INT) ||
            (tpe == LogicalTypeRoot.BIGINT && atpe == Schema.Type.LONG) ||
            (tpe == LogicalTypeRoot.FLOAT && atpe == Schema.Type.FLOAT) ||
            (tpe == LogicalTypeRoot.DOUBLE && atpe == Schema.Type.DOUBLE) ||
            (tpe == LogicalTypeRoot.VARBINARY && atpe == BYTES)) {
            return (getter, ordinal) -> getter.getField(ordinal);
        } else if (tpe == LogicalTypeRoot.DECIMAL && (atpe == FIXED || atpe == BYTES)) {
            val d = (DecimalType) dataType.getLogicalType();
            if (avroType.getLogicalType() == LogicalTypes.decimal(d.getPrecision(), d.getScale())) {
                return (getter, ordinal) -> {
                    val decimal = (java.math.BigDecimal) getter.getField(ordinal);
                    return decimalConversion.toFixed(
                        decimal,
                        avroType,
                        LogicalTypes.decimal(d.getPrecision(), d.getScale()));
                };
            } else {
                throw new SchemaUtils.IncompatibleSchemaException(
                    "Cannot convert flink decimal type to Avro logical type");
            }
        } else if (tpe == LogicalTypeRoot.BIGINT && atpe == BYTES) {
            return (getter, ordinal) -> ByteBuffer.wrap((byte[]) getter.getField(ordinal));
        } else if (tpe == LogicalTypeRoot.DATE && atpe == Schema.Type.INT) {
            return (getter, ordinal) -> DateTimeUtils.fromJavaDate((java.sql.Date) getter.getField(ordinal));
        } else if (tpe == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE && atpe == Schema.Type.LONG) {
            val altpe = avroType.getLogicalType();
            if (altpe instanceof LogicalTypes.TimestampMillis) {
                return (getter, ordinal) -> DateTimeUtils.fromJavaTimestamp((java.sql.Timestamp) getter.getField(ordinal)) / 1000;
            } else if (altpe instanceof LogicalTypes.TimestampMicros) {
                return (getter, ordinal) -> DateTimeUtils.fromJavaTimestamp((java.sql.Timestamp) getter.getField(ordinal));
            } else {
                throw new SchemaUtils.IncompatibleSchemaException(
                    "Cannot convert flink timestamp to avro logical type " + altpe.toString());
            }
        } else if (tpe == LogicalTypeRoot.VARCHAR && atpe == STRING) {
            return (getter, ordinal) -> new Utf8((String) getter.getField(ordinal));
        } else if (tpe == LogicalTypeRoot.VARCHAR && atpe == ENUM) {
            val enumSymbols = new HashSet<>(avroType.getEnumSymbols());
            return (getter, ordinal) -> {
                val data = (String) getter.getField(ordinal);
                if (!enumSymbols.contains(data)) {
                    throw new RuntimeException(String.format(
                        "Cannot write %s since it's not defined in enum %s", data, String.join(", ", enumSymbols)));
                }
                return new GenericData.EnumSymbol(avroType, data);
            };
        } else if (tpe == LogicalTypeRoot.ARRAY && atpe == ARRAY && dataType instanceof CollectionDataType) {
            val et = ((CollectionDataType) dataType).getElementDataType();
            val containsNull = et.getLogicalType().isNullable();
            val elementConverter = newConverter(et, resolveNullableType(avroType.getElementType(), containsNull));
            return (getter, ordinal) -> {
                val arrayData = (Object[]) getter.getField(ordinal);
                val len = arrayData.length;
                val result = new Object[len];
                for (int i = 0; i < len; i++) {
                    if (containsNull && arrayData[i] == null) {
                        result[i] = null;
                    } else {
                        result[i] = elementConverter.apply(new PositionedGetter(arrayData), i);
                    }
                }
                // avro writer is expecting a Java Collection, so we convert it into
                // `ArrayList` backed by the specified array without data copying.
                return Arrays.asList(result);
            };
        } else if (tpe == LogicalTypeRoot.MAP && atpe == MAP &&
            ((KeyValueDataType) dataType).getKeyDataType().getLogicalType().getTypeRoot() == LogicalTypeRoot.VARCHAR) {

            val kvt = (KeyValueDataType) dataType;
            val ktl = kvt.getKeyDataType().getLogicalType();
            val vt = kvt.getValueDataType();
            val vtl = kvt.getValueDataType().getLogicalType();
            val valueContainsNull = vt.getLogicalType().isNullable();

            val valueConverter =
                newConverter(vt, resolveNullableType(avroType.getValueType(), valueContainsNull));

            return (getter, ordinal) -> getter.getField(ordinal);
        } else if (tpe == LogicalTypeRoot.ROW && atpe == RECORD) {
            val st = (FieldsDataType) dataType;
            val structConverter = newStructConverter(st, avroType);
            return (getter, ordinal) -> ((GenericAvroRecord) structConverter.apply(getter.getField(ordinal))).getAvroRecord();
        } else {
            throw new SchemaUtils.IncompatibleSchemaException(String.format(
                "Cannot convert flink type %s to avro type %s", dataType.toString(), avroType.toString(true)));
        }
    }

    private List<Field> getFields(Schema aschema) {
        return aschema.getFields().stream()
            .map(f -> new Field(f.name(), f.pos())).collect(Collectors.toList());
    }

    private Schema resolveNullableType(Schema avroType, boolean nullable) {
        if (nullable && avroType.getType() != Schema.Type.NULL) {
            val fields = avroType.getTypes();
            assert fields.size() == 2;
            val actualType = fields.stream()
                .filter(f -> f.getType() != Schema.Type.NULL).collect(Collectors.toList());
            assert actualType.size() == 1;
            return actualType.get(0);
        } else {
            return avroType;
        }
    }

    public static class PositionedGetter {
        private final Object[] array;
        private final Row row;


        public PositionedGetter(Object[] array, Row row) {
            this.array = array;
            this.row = row;
        }

        public PositionedGetter(Row row) {
            this(null, row);
        }

        public PositionedGetter(Object[] array) {
            this(array, null);
        }

        public Object getField(int i) {
            if (array != null) {
                return array[i];
            } else {
               return row.getField(i);
            }
        }
    }
}
