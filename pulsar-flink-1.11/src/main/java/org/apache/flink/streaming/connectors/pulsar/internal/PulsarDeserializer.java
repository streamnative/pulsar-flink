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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableSet;
import org.apache.pulsar.shade.org.apache.avro.Conversions;
import org.apache.pulsar.shade.org.apache.avro.LogicalType;
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.SchemaBuilder;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericFixed;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.shade.org.apache.avro.util.Utf8;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.META_FIELD_NAMES;
import static org.apache.pulsar.common.schema.SchemaType.JSON;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.ARRAY;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.FLOAT;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.INT;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.LONG;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.MAP;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.NULL;
import static org.apache.pulsar.shade.org.apache.avro.Schema.Type.UNION;

/**
 * Deserialize Pulsar message into Flink row.
 */
@Slf4j
public class PulsarDeserializer {

    private final SchemaInfo schemaInfo;

    private final JSONOptions parsedOptions;

    private final Function<Message<?>, Row> converter;

    private final DataType rootDataType;

    private final Conversions.DecimalConversion decimalConversions =
            new Conversions.DecimalConversion();

    public PulsarDeserializer(SchemaInfo schemaInfo, JSONOptions parsedOptions) {
        try {
            this.schemaInfo = schemaInfo;
            this.parsedOptions = parsedOptions;
            this.rootDataType = SchemaUtils.si2SqlType(schemaInfo);

            switch (JSON) {
                case AVRO:
                    FieldsDataType st = (FieldsDataType) rootDataType;
                    int fieldsNum = st.getChildren().size() + META_FIELD_NAMES.size();
                    RowUpdater fieldUpdater = new RowUpdater();
                    Schema avroSchema =
                            new Schema.Parser().parse(new String(schemaInfo.getSchema(), StandardCharsets.UTF_8));
                    BinFunction<RowUpdater, GenericRecord> writer = getRecordWriter(avroSchema, st, new ArrayList<>());
                    this.converter = msg -> {
                        Row resultRow = new Row(fieldsNum);
                        fieldUpdater.setRow(resultRow);
                        Object value = msg.getValue();
                        writer.apply(fieldUpdater, ((GenericAvroRecord) value).getAvroRecord());
                        writeMetadataFields(msg, resultRow);
                        return resultRow;
                    };
                    break;

                case JSON:
                    FieldsDataType fdt = (FieldsDataType) rootDataType;
                    BiFunction<JsonFactory, String, JsonParser> createParser =
                            (jsonFactory, s) -> {
                                try {
                                    return jsonFactory.createParser(s);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            };
                    JacksonRecordParser rawParser = new JacksonRecordParser(rootDataType, parsedOptions);
                    JacksonRecordParser.FailureSafeRecordParser parser = new JacksonRecordParser.FailureSafeRecordParser(
                            (s, row) -> rawParser.parse(s, createParser, row),
                            parsedOptions.getParseMode(),
                            fdt);
                    this.converter = msg -> {
                        Row resultRow = new Row(fdt.getChildren().size() + META_FIELD_NAMES.size());
                        byte[] value = msg.getData();
                        parser.parse(new String(value, StandardCharsets.UTF_8), resultRow);
                        writeMetadataFields(msg, resultRow);
                        return resultRow;
                    };
                    break;

                default:
                    RowUpdater fUpdater = new RowUpdater();
                    TriFunction<RowUpdater, Integer, Object> writer2 = newAtomicWriter(rootDataType);
                    this.converter = msg -> {
                        Row tmpRow = new Row(1 + META_FIELD_NAMES.size());
                        fUpdater.setRow(tmpRow);
                        Object value = msg.getValue();
                        writer2.apply(fUpdater, 0, value);
                        writeMetadataFields(msg, tmpRow);
                        return tmpRow;
                    };
            }

        } catch (SchemaUtils.IncompatibleSchemaException e) {
            log.error("Failed to convert pulsar schema to flink data type {}",
                    ExceptionUtils.stringifyException(e));
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
        LogicalTypeRoot tpe = dataType.getLogicalType().getTypeRoot();

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
        LogicalTypeRoot tpe = flinkType.getLogicalType().getTypeRoot();
        Schema.Type atpe = avroType.getType();

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
            LogicalType altpe = avroType.getLogicalType();
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
                    Utf8 u8 = (Utf8) value;
                    byte[] bytes = new byte[u8.getByteLength()];
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
                    ByteBuffer bb = (ByteBuffer) value;
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
            DecimalType d = (DecimalType) flinkType.getLogicalType();
            return (rowUpdater, ordinal, value) -> {
                BigDecimal bigDecimal = decimalConversions.fromFixed(
                        (GenericFixed) value,
                        avroType,
                        LogicalTypes.decimal(d.getPrecision(), d.getScale()));
                rowUpdater.set(ordinal, bigDecimal);
            };

        } else if (atpe == Schema.Type.BYTES && tpe == LogicalTypeRoot.DECIMAL) {
            DecimalType d = (DecimalType) flinkType.getLogicalType();
            return (rowUpdater, ordinal, value) -> {
                BigDecimal bigDecimal = decimalConversions.fromBytes(
                        (ByteBuffer) value,
                        avroType,
                        LogicalTypes.decimal(d.getPrecision(), d.getScale()));
                rowUpdater.set(ordinal, bigDecimal);
            };

        } else if (atpe == Schema.Type.RECORD && tpe == LogicalTypeRoot.ROW) {
            FieldsDataType fieldsDataType = (FieldsDataType) flinkType;
            BinFunction<RowUpdater, GenericRecord> writeRecord = getRecordWriter(avroType, fieldsDataType, path);
            return (rowUpdater, ordinal, value) -> {
                Row row = new Row(fieldsDataType.getChildren().size());
                RowUpdater ru = new RowUpdater();
                ru.setRow(row);
                writeRecord.apply(ru, (GenericRecord) value);
                rowUpdater.set(ordinal, row);
            };

        } else if (tpe == LogicalTypeRoot.ARRAY && atpe == ARRAY && flinkType instanceof CollectionDataType) {
            DataType et = ((CollectionDataType) flinkType).getElementDataType();
            boolean containsNull = et.getLogicalType().isNullable();
            TriFunction<FlinkDataUpdater, Integer, Object> elementWriter = newWriter(avroType.getElementType(), et, path);
            return (rowUpdater, ordinal, value) -> {
                List array = (List) value;
                int len = array.size();
                Object[] result = new Object[len];
                ArrayDataUpdater elementUpdater = new ArrayDataUpdater(result);

                for (int i = 0; i < len; i++) {
                    Object element = array.get(i);
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

            KeyValueDataType kvt = (KeyValueDataType) flinkType;
            DataType kt = kvt.getKeyDataType();
            TriFunction<FlinkDataUpdater, Integer, Object> keyWriter = newWriter(SchemaBuilder.builder().stringType(), kt, path);
            DataType vt = kvt.getValueDataType();
            TriFunction<FlinkDataUpdater, Integer, Object> valueWriter = newWriter(avroType.getValueType(), vt, path);
            boolean valueContainsNull = vt.getLogicalType().isNullable();

            return (rowUpdater, ordinal, value) -> {
                Map<Object, Object> map = (Map<Object, Object>) value;
                String[] keys = new String[map.size()];
                Object[] values = new Object[map.size()];
                ArrayDataUpdater keyUpdater = new ArrayDataUpdater(keys);
                ArrayDataUpdater valueUpdater = new ArrayDataUpdater(values);

                Iterator<Map.Entry<Object, Object>> iterator = map.entrySet().iterator();
                int i = 0;
                while (iterator.hasNext()) {
                    Map.Entry entry = iterator.next();
                    assert entry.getKey() != null;
                    keyWriter.apply(keyUpdater, i, entry.getKey());

                    if (entry.getValue() == null) {
                        if (!valueContainsNull) {
                            throw new RuntimeException(String.format(
                                    "Map value at path %s is not allowed to be null", path.toString()));
                        } else {
                            valueUpdater.setNullAt(i);
                        }
                    } else {
                        valueWriter.apply(valueUpdater, i, entry.getValue());
                    }
                    i += 1;
                }

                Map<String, Object> result = new HashMap<>(map.size());
                for (int j = 0; j < map.size(); j++) {
                    result.put(keys[j], values[j]);
                }

                rowUpdater.set(ordinal, result);
            };

        } else if (atpe == UNION) {
            List<Schema> allTypes = avroType.getTypes();
            List<Schema> nonNullTypes = allTypes.stream().filter(t -> t.getType() != NULL).collect(Collectors.toList());
            if (!nonNullTypes.isEmpty()) {

                if (nonNullTypes.size() == 1) {
                    return newWriter(nonNullTypes.get(0), flinkType, path);
                } else {
                    if (nonNullTypes.size() == 2) {
                        Schema.Type tp1 = nonNullTypes.get(0).getType();
                        Schema.Type tp2 = nonNullTypes.get(1).getType();
                        if (ImmutableSet.of(tp1, tp2).equals(ImmutableSet.of(INT, LONG)) && flinkType == DataTypes.BIGINT()) {
                            return (updater, ordinal, value) -> {
                                if (value == null) {
                                    updater.setNullAt(ordinal);
                                } else if (value instanceof Long) {
                                    updater.set(ordinal, value);
                                } else if (value instanceof Integer) {
                                    updater.set(ordinal, ((Integer) value).longValue());
                                }
                            };
                        } else if (ImmutableSet.of(tp1, tp2).equals(ImmutableSet.of(FLOAT, DOUBLE)) && flinkType == DataTypes.DOUBLE()) {
                            return (updater, ordinal, value) -> {
                                if (value == null) {
                                    updater.setNullAt(ordinal);
                                } else if (value instanceof Double) {
                                    updater.set(ordinal, value);
                                } else if (value instanceof Float) {
                                    updater.set(ordinal, ((Float) value).doubleValue());
                                }
                            };
                        } else {
                            throw new SchemaUtils.IncompatibleSchemaException(String.format(
                                    "Cannot convert %s %s together to %s", tp1.toString(), tp2.toString(), flinkType.toString()));
                        }
                    } else if (tpe == LogicalTypeRoot.ROW && ((RowType) flinkType.getLogicalType()).getFieldCount() == nonNullTypes.size()) {
                        RowType rt = (RowType) flinkType.getLogicalType();

                        List<TriFunction<FlinkDataUpdater, Integer, Object>> fieldWriters = new ArrayList<TriFunction<FlinkDataUpdater, Integer, Object>>();
                        for (int i = 0; i < nonNullTypes.size(); i++) {
                            Schema schema = nonNullTypes.get(i);
                            String field = rt.getFieldNames().get(i);
                            org.apache.flink.table.types.logical.LogicalType logicalType = rt.getTypeAt(i);
                            fieldWriters.add(newWriter(schema, TypeConversions.fromLogicalToDataType(logicalType),
                                    Stream.concat(path.stream(), Stream.of(field)).collect(Collectors.toList())));
                        }
                        return (updater, ordinal, value) -> {
                            Row row = new Row(rt.getFieldCount());
                            RowUpdater fieldUpdater = new RowUpdater();
                            fieldUpdater.setRow(row);
                            int i = GenericData.get().resolveUnion(avroType, value);
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
        List<Integer> validFieldIndexes = new ArrayList<>();
        List<BinFunction<RowUpdater, Object>> fieldWriters = new ArrayList<>();

        int length = sqlType.getChildren().size();
        RowType rowType = (RowType) sqlType.getLogicalType();
        List<RowType.RowField> fields = rowType.getFields();
        //Map<String, DataType> fieldsType = sqlType.getFieldDataTypes();

        for (int i = 0; i < length; i++) {
            RowType.RowField sqlField = fields.get(i);
            org.apache.flink.table.types.logical.LogicalType logicalType = rowType.getTypeAt(i);
            Schema.Field avroField = avroType.getField(sqlField.getName());
            if (avroField != null) {
                validFieldIndexes.add(avroField.pos());

                TriFunction<FlinkDataUpdater, Integer, Object> baseWriter = newWriter(
                        avroField.schema(), TypeConversions.fromLogicalToDataType(logicalType),
                        Stream.concat(path.stream(), Stream.of(sqlField.getName())).collect(Collectors.toList()));
                int ordinal = i;

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

    /**
     * Update flink data object.
     */
    interface FlinkDataUpdater {
        void set(int ordinal, Object value);

        void setNullAt(int ordinal);
    }

    /**
     * Flink Row field updater.
     */
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

    /**
     * Flink array field updater.
     */
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

    /**
     * Trinary function interface that takes three arguments and returns nothing.
     *
     * @param <A> type of the first argument.
     * @param <B> type of the second argument.
     * @param <C> type of the third argument.
     */
    public interface TriFunction<A, B, C> {

        /**
         * Applies this function to the given arguments.
         */
        void apply(A a, B b, C c);
    }

    /**
     * Binary function interface that takes three arguments and returns nothing.
     *
     * @param <A> type of the first argument.
     * @param <B> type of the second argument.
     */
    public interface BinFunction<A, B> {

        /**
         * Applies this function to the given arguments.
         */
        void apply(A a, B b);
    }
}
