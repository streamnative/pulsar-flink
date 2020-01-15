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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableSet;
import org.apache.pulsar.shade.org.apache.avro.Conversions;
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericFixed;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.shade.org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.META_FIELD_NAMES;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.ARRAY;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.FLOAT;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.INT;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.LONG;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.MAP;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.NULL;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.UNION;

@Slf4j
public class PulsarDeserializer {

    private final SchemaInfo schemaInfo;

    private final JSONOptions parsedOptions;

    private final Function<Message<?>, Row> converter;

    private final DataType rootDataType;

    @Getter(lazy = true)
    private final Conversions.DecimalConversion decimalConversions =
        new Conversions.DecimalConversion();

    public PulsarDeserializer(SchemaInfo schemaInfo, JSONOptions parsedOptions) {
        try {
            this.schemaInfo = schemaInfo;
            this.parsedOptions = parsedOptions;
            this.rootDataType = SchemaUtils.si2SqlType(schemaInfo);

            switch (schemaInfo.getType()) {
                case AVRO:
                    val st = (FieldsDataType) rootDataType;
                    val fieldsNum = st.getFieldDataTypes().size() + META_FIELD_NAMES.size();
                    val fieldUpdater = new RowUpdater();
                    val avroSchema =
                        new Schema.Parser().parse(new String(schemaInfo.getSchema(), StandardCharsets.UTF_8));
                    val writer = getRecordWriter(avroSchema, st, new ArrayList<>());
                    this.converter = msg -> {
                        val resultRow = new Row(fieldsNum);
                        fieldUpdater.setRow(resultRow);
                        val value = msg.getValue();
                        writer.apply(fieldUpdater, ((GenericAvroRecord) value).getAvroRecord());
                        writeMetadataFields(msg, resultRow);
                        return resultRow;
                    };
                    break;

                case JSON:
                    val fdt = (FieldsDataType) rootDataType;
                    BiFunction<JsonFactory, String, JsonParser> createParser =
                        (jsonFactory, s) -> {
                            try {
                                return jsonFactory.createParser(s);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        };
                    val rawParser = new JacksonRecordParser(rootDataType, parsedOptions);
                    val parser = new JacksonRecordParser.FailureSafeRecordParser(
                        (s, row) -> rawParser.parse(s, createParser, row),
                        parsedOptions.getParseMode(),
                        fdt);
                    this.converter = msg -> {
                        val resultRow = new Row(fdt.getFieldDataTypes().size() + META_FIELD_NAMES.size());
                        val value = msg.getData();
                        parser.parse(new String(value, StandardCharsets.UTF_8), resultRow);
                        writeMetadataFields(msg, resultRow);
                        return resultRow;
                    };
                    break;

                default:
                    val fUpdater = new RowUpdater();
                    val writer2 = newAtomicWriter(rootDataType);
                    this.converter = msg -> {
                        val tmpRow = new Row(1 + META_FIELD_NAMES.size());
                        fUpdater.setRow(tmpRow);
                        val value = msg.getValue();
                        writer2.apply(fUpdater, 0, value);
                        writeMetadataFields(msg, tmpRow);
                        return tmpRow;
                    };
            }

        } catch (SchemaUtils.IncompatibleSchemaException e) {
            log.error("Failed to convert pulsar schema to flink data type %s",
                ExceptionUtils.getFullStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public Row deserialize(Message<?> message) {
        return converter.apply(message);
    }

    private void writeMetadataFields(Message<?> message, Row row) {
        int metaStartIdx = row.getArity() - 5;

        if (message.hasKey()) {
            row.setField(metaStartIdx, message.getKeyBytes());
        } else {
            row.setField(metaStartIdx, null);
        }

        row.setField(metaStartIdx + 1, message.getTopicName());
        row.setField(metaStartIdx + 2, message.getMessageId().toByteArray());
        row.setField(metaStartIdx + 3, new Timestamp(message.getPublishTime()));

        if (message.getEventTime() > 0L) {
            row.setField(metaStartIdx + 4, new Timestamp(message.getEventTime()));
        } else {
            row.setField(metaStartIdx + 4, null);
        }
    }

    private TriFunction<RowUpdater, Integer, Object> newAtomicWriter(DataType dataType) {
        val tpe = dataType.getLogicalType().getTypeRoot();

        switch (tpe) {
            case DATE:
                return (rowUpdater, ordinal, value) -> {
                    rowUpdater.set(ordinal,
                        ((java.util.Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
                };
            default:
                return (rowUpdater, ordinal, value) -> rowUpdater.set(ordinal, value);
        }
    }


    private TriFunction<FlinkDataUpdater, Integer, Object> newWriter(Schema avroType, DataType flinkType, List<String> path) throws SchemaUtils.IncompatibleSchemaException {
        val tpe = flinkType.getLogicalType().getTypeRoot();
        val atpe = avroType.getType();

        if (atpe == Schema.Type.NULL && tpe == LogicalTypeRoot.NULL) {
            return (rowUpdater, ordinal, value) -> rowUpdater.setNullAt(ordinal);

        } else if (atpe == Schema.Type.BOOLEAN && tpe == LogicalTypeRoot.BOOLEAN ||
                atpe == Schema.Type.INT && tpe == LogicalTypeRoot.INTEGER ||
                atpe == Schema.Type.LONG && tpe == LogicalTypeRoot.BIGINT ||
                atpe == Schema.Type.FLOAT && tpe == LogicalTypeRoot.FLOAT ||
                atpe == Schema.Type.DOUBLE && tpe == LogicalTypeRoot.DOUBLE) {
            return (rowUpdater, ordinal, value) -> rowUpdater.set(ordinal, value);

        } else if (atpe == Schema.Type.INT && tpe == LogicalTypeRoot.DATE) {
            return (rowUpdater, ordinal, value) ->
                rowUpdater.set(ordinal,
                    DateTimeUtils.toJavaDate((Integer) value));

        } else if (atpe == Schema.Type.LONG && tpe == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            val altpe = avroType.getLogicalType();
            if (altpe instanceof LogicalTypes.TimestampMillis) {
                return (rowUpdater, ordinal, value) ->
                    rowUpdater.set(ordinal,
                        DateTimeUtils.toJavaTimestamp(((Long) value) * 1000));
            } else if (altpe instanceof LogicalTypes.TimestampMicros) {
                return (rowUpdater, ordinal, value) ->
                    rowUpdater.set(ordinal,
                        DateTimeUtils.toJavaTimestamp((Long) value));
            } else {
                throw new SchemaUtils.IncompatibleSchemaException(String.format(
                    "Cannot convert Avro logical type %s to flink timestamp type", altpe.toString()));
            }

        } else if (atpe == Schema.Type.STRING && tpe == LogicalTypeRoot.VARCHAR) {
            return (rowUpdater, ordinal, value) -> {
                String s = null;
                if (value instanceof String) {
                    s = (String) value;
                } else if (value instanceof Utf8) {
                    val u8 = (Utf8) value;
                    val bytes = new byte[u8.getByteLength()];
                    System.arraycopy(u8.getBytes(), 0, bytes, 0, u8.getByteLength());
                    s = new String(bytes, StandardCharsets.UTF_8);
                }
                rowUpdater.set(ordinal, s);
            };

        } else if (atpe == Schema.Type.ENUM && tpe == LogicalTypeRoot.VARCHAR) {
            return (rowUpdater, ordinal, value) ->
                rowUpdater.set(ordinal, value.toString());

        } else if (atpe == Schema.Type.FIXED && tpe == LogicalTypeRoot.BINARY) {
            return (rowUpdater, ordinal, value) ->
                rowUpdater.set(ordinal, ((GenericFixed) value).bytes().clone());

        } else if (atpe == Schema.Type.BYTES && tpe == LogicalTypeRoot.VARBINARY) {
            return (rowUpdater, ordinal, value) -> {
                byte[] bytes = null;
                if (value instanceof ByteBuffer) {
                    val bb = (ByteBuffer) value;
                    bytes = new byte[bb.remaining()];
                    bb.get(bytes);
                } else if (value instanceof byte[]) {
                    bytes = (byte[]) value;
                } else {
                    throw new RuntimeException(value.toString() + " is not a valid avro binary");
                }
                rowUpdater.set(ordinal, bytes);
            };

        } else if (atpe == Schema.Type.FIXED && tpe == LogicalTypeRoot.DECIMAL) {
            val d = (DecimalType) flinkType.getLogicalType();
            return (rowUpdater, ordinal, value) -> {
                val bigDecimal = decimalConversions.fromFixed(
                    (GenericFixed) value,
                    avroType,
                    LogicalTypes.decimal(d.getPrecision(), d.getScale()));
                rowUpdater.set(ordinal, bigDecimal);
            };

        } else if (atpe == Schema.Type.BYTES && tpe == LogicalTypeRoot.DECIMAL) {
            val d = (DecimalType) flinkType.getLogicalType();
            return (rowUpdater, ordinal, value) -> {
                val bigDecimal = decimalConversions.fromBytes(
                    (ByteBuffer) value,
                    avroType,
                    LogicalTypes.decimal(d.getPrecision(), d.getScale()));
                rowUpdater.set(ordinal, bigDecimal);
            };

        } else if (atpe == Schema.Type.RECORD && tpe == LogicalTypeRoot.ROW) {
            val fieldsDataType = (FieldsDataType) flinkType;
            val writeRecord = getRecordWriter(avroType, fieldsDataType, path);
            return (rowUpdater, ordinal, value) -> {
                val row = new Row(fieldsDataType.getFieldDataTypes().size());
                val ru = new RowUpdater();
                ru.setRow(row);
                writeRecord.apply(ru, (GenericRecord) value);
                rowUpdater.set(ordinal, row);
            };

        } else if (tpe == LogicalTypeRoot.ARRAY && atpe == ARRAY && flinkType instanceof CollectionDataType) {
            val et = ((CollectionDataType) flinkType).getElementDataType();
            val containsNull = et.getLogicalType().isNullable();
            val elementWriter = newWriter(avroType.getElementType(), et, path);
            return (rowUpdater, ordinal, value) -> {
                val array = (Object[]) value;
                val len = array.length;
                val result = new Object[len];
                val elementUpdater = new ArrayDataUpdater(result);

                for (int i = 0; i <len; i++) {
                    val element = array[i];
                    if (element == null) {
                        if (!containsNull) {
                            throw new RuntimeException(String.format(
                                "Array value at path %s is not allowed to be null", path.toString()));
                        } else {
                            elementUpdater.setNullAt(i);
                        }
                    } else {
                        elementWriter.apply(elementUpdater, i, element);
                    }
                }

                rowUpdater.set(ordinal, result);
            };

        } else if (tpe == LogicalTypeRoot.MAP && atpe == MAP &&
                ((KeyValueDataType) flinkType).getKeyDataType().getLogicalType().getTypeRoot() == LogicalTypeRoot.VARCHAR) {

            val kvt = (KeyValueDataType) flinkType;
            val kt = kvt.getKeyDataType();
            val vt = kvt.getValueDataType();
            val valueContainsNull = vt.getLogicalType().isNullable();

            return (rowUpdater, ordinal, value) -> {
                val map = (Map<Object, Object>) value;
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    assert entry.getKey() != null;
                    if (entry.getValue() == null) {
                        if (!valueContainsNull) {
                            throw new RuntimeException(String.format(
                                "Map value at path %s is not allowed to be null", path.toString()));
                        }
                    }
                }
                rowUpdater.set(ordinal, map);
            };
        } else if (atpe == UNION) {
            val allTypes = avroType.getTypes();
            val nonNullTypes = allTypes.stream().filter(t -> t.getType() != NULL).collect(Collectors.toList());
            if (!nonNullTypes.isEmpty()) {

                if (nonNullTypes.size() == 1) {
                    return newWriter(nonNullTypes.get(0), flinkType, path);
                } else {
                    if (nonNullTypes.size() == 2) {
                        val tp1 = nonNullTypes.get(0).getType();
                        val tp2 = nonNullTypes.get(1).getType();
                        if (ImmutableSet.of(tp1, tp2).equals(ImmutableSet.of(INT, LONG)) && flinkType == DataTypes.BIGINT()) {
                            return (updater, ordinal, value) -> {
                                if (value == null) {
                                    updater.setNullAt(ordinal);
                                } else if (value instanceof Long) {
                                    updater.set(ordinal, (Long) value);
                                } else if (value instanceof Integer) {
                                    updater.set(ordinal, ((Integer) value).longValue());
                                }
                            };
                        } else if (ImmutableSet.of(tp1, tp2).equals(ImmutableSet.of(FLOAT, DOUBLE)) && flinkType == DataTypes.DOUBLE()) {
                            return (updater, ordinal, value) -> {
                                if (value == null) {
                                    updater.setNullAt(ordinal);
                                } else if (value instanceof Double) {
                                    updater.set(ordinal, (Double) value);
                                } else if (value instanceof Float) {
                                    updater.set(ordinal, ((Float) value).doubleValue());
                                }
                            };
                        } else {
                            throw new SchemaUtils.IncompatibleSchemaException(String.format(
                                "Cannot convert %s %s together to %s", tp1.toString(), tp2.toString(), flinkType.toString()));
                        }
                    } else if (tpe == LogicalTypeRoot.ROW && ((RowType) flinkType.getLogicalType()).getFieldCount() == nonNullTypes.size()) {
                        val feildsType = ((FieldsDataType) flinkType).getFieldDataTypes();
                        val rt = (RowType) flinkType.getLogicalType();

                        val fieldWriters = new ArrayList<TriFunction<FlinkDataUpdater, Integer, Object>>();
                        for (int i = 0; i < nonNullTypes.size(); i++) {
                            val schema = nonNullTypes.get(i);
                            val field = rt.getFieldNames().get(i);
                            fieldWriters.add(newWriter(schema, feildsType.get(field),
                                Stream.concat(path.stream(), Stream.of(field)).collect(Collectors.toList())));
                        }
                        return (updater, ordinal, value) -> {
                            val row = new Row(rt.getFieldCount());
                            val fieldUpdater = new RowUpdater();
                            fieldUpdater.setRow(row);
                            val i = GenericData.get().resolveUnion(avroType, value);
                            fieldWriters.get(i).apply(fieldUpdater, i, value);
                            updater.set(ordinal, row);
                        };
                    } else {
                        throw new SchemaUtils.IncompatibleSchemaException(String.format(
                            "Cannot convert avro to flink because schema at %s is not compatible (avroType = %s, sqlType = %s)",
                            path.toString(), avroType.toString(), flinkType.toString()));
                    }
                }

            } else {
                return (updater, ordinal, value) -> updater.setNullAt(ordinal);
            }
        } else {
            throw new SchemaUtils.IncompatibleSchemaException(String.format(
                "Cannot convert avro to flink because schema at path %s is not compatible (avroType = %s, sqlType = %s)",
                path.toString(), avroType.toString(), flinkType.toString()));
        }

    }

    private BinFunction<RowUpdater, GenericRecord> getRecordWriter(Schema avroType, FieldsDataType sqlType, List<String> path) throws SchemaUtils.IncompatibleSchemaException {
        val validFieldIndexes = new ArrayList<Integer>();
        val fieldWriters = new ArrayList<BinFunction<RowUpdater, Object>>();

        val length = sqlType.getFieldDataTypes().size();
        val fields = ((RowType) sqlType.getLogicalType()).getFields();
        val fieldsType = sqlType.getFieldDataTypes();

        for (int i = 0; i < length; i++) {
            val sqlField = fields.get(i);
            val avroField = avroType.getField(sqlField.getName());
            if (avroField != null) {
                validFieldIndexes.add(avroField.pos());

                val baseWriter = newWriter(
                    avroField.schema(), fieldsType.get(sqlField.getName()),
                    Stream.concat(path.stream(), Stream.of(sqlField.getName())).collect(Collectors.toList()));
                val ordinal = i;

                BinFunction<RowUpdater, Object> fieldWriter = (updater, value) -> {
                    if (value == null) {
                        updater.setNullAt(ordinal);
                    } else {
                        baseWriter.apply(updater, ordinal, value);
                    }
                };

                fieldWriters.add(fieldWriter);

            } else if (!sqlField.getType().isNullable()) {
                throw new SchemaUtils.IncompatibleSchemaException(String.format(
                    "Cannot find non-nullable field in avro schema %s", avroType));
            }
        }

        return (rowUpdater, record) -> {
            for (int i = 0; i < validFieldIndexes.size(); i++) {
                fieldWriters.get(i).apply(rowUpdater, record.get(validFieldIndexes.get(i)));
            }
        };
    }


    static interface FlinkDataUpdater {
        void set(int ordinal, Object value);

        void setNullAt(int ordinal);
    }

    public static final class RowUpdater implements FlinkDataUpdater {

        private Row row;

        public void setRow(Row currentRow) {
            this.row = currentRow;
        }

        @Override
        public void set(int ordinal, Object value) {
            row.setField(ordinal, value);
        }

        @Override
        public void setNullAt(int ordinal) {
            row.setField(ordinal, null);
        }
    }

    public static final class ArrayDataUpdater implements FlinkDataUpdater {

        private final Object[] array;

        public ArrayDataUpdater(Object[] array) {
            this.array = array;
        }

        @Override
        public void set(int ordinal, Object value) {
            array[ordinal] = value;
        }

        @Override
        public void setNullAt(int ordinal) {
            array[ordinal] = null;
        }
    }

    public interface TriFunction<A, B, C> {

        /**
         * Applies this function to the given arguments.
         *
         * @return the function result
         */
        void apply(A a, B b, C c);
    }

    public interface BinFunction<A, B> {

        /**
         * Applies this function to the given arguments.
         *
         * @return the function result
         */
        void apply(A a, B b);
    }
}
