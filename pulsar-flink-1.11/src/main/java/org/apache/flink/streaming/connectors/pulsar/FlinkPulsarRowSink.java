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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroFormatFactory;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.DateTimeUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.EVENT_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.KEY_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.META_FIELD_NAMES;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Write Flink Row to Pulsar.
 */
@Slf4j
@Deprecated
public class FlinkPulsarRowSink extends FlinkPulsarSinkBase<Row> {

    protected final DataType dataType;

    private DataType valueType;

    private SerializableFunction<Row, Row> valueProjection;

    private SerializableFunction<Row, Row> metaProjection;

    public FlinkPulsarRowSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            SerializationSchema serializationSchema,
            DataType dataType) {
        super(
                adminUrl,
                defaultTopicName,
                clientConf,
                properties,
                new RowSinkSerializationSchema(defaultTopicName.get(), serializationSchema, RecordSchemaType.AVRO, dataType));
        this.dataType = dataType;
        createProjection();
    }

    public FlinkPulsarRowSink(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            SerializationSchema<Row> serializationSchema,
            DataType dataType) {
        this(adminUrl, defaultTopicName, PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
                properties, serializationSchema, dataType);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //this.serializer = new PulsarSerializer(valueType, false);
    }

    private void createProjection() {
        int[] metas = new int[3];

        FieldsDataType fdt = (FieldsDataType) dataType;
        RowType rowType = (RowType) fdt.getLogicalType();
        //Map<String, DataType> fdtm = fdt.getFieldDataTypes();

        List<RowType.RowField> rowFields = ((RowType) fdt.getLogicalType()).getFields();
        Map<String, Tuple2<LogicalTypeRoot, Integer>> name2Type = new HashMap<>();
        for (int i = 0; i < rowFields.size(); i++) {
            RowType.RowField rf = rowFields.get(i);
            name2Type.put(rf.getName(), new Tuple2<>(rf.getType().getTypeRoot(), i));
        }

        // topic
        if (name2Type.containsKey(TOPIC_ATTRIBUTE_NAME)) {
            Tuple2<LogicalTypeRoot, Integer> value = name2Type.get(TOPIC_ATTRIBUTE_NAME);
            if (value.f0 == LogicalTypeRoot.VARCHAR) {
                metas[0] = value.f1;
            } else {
                throw new IllegalStateException(
                        String.format("attribute unsupported type %s, %s must be a string", value.f0.toString(), TOPIC_ATTRIBUTE_NAME));
            }
        } else {
            if (!forcedTopic) {
                throw new IllegalStateException(
                        String.format("topic option required when no %s attribute is present.", TOPIC_ATTRIBUTE_NAME));
            }
            metas[0] = -1;
        }

        // key
        if (name2Type.containsKey(KEY_ATTRIBUTE_NAME)) {
            Tuple2<LogicalTypeRoot, Integer> value = name2Type.get(KEY_ATTRIBUTE_NAME);
            if (value.f0 == LogicalTypeRoot.VARBINARY) {
                metas[1] = value.f1;
            } else {
                throw new IllegalStateException(
                        String.format("%s attribute unsupported type %s", KEY_ATTRIBUTE_NAME, value.f0.toString()));
            }
        } else {
            metas[1] = -1;
        }

        // eventTime
        if (name2Type.containsKey(EVENT_TIME_NAME)) {
            Tuple2<LogicalTypeRoot, Integer> value = name2Type.get(EVENT_TIME_NAME);
            if (value.f0 == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                metas[2] = value.f1;
            } else {
                throw new IllegalStateException(
                        String.format("%s attribute unsupported type %s", EVENT_TIME_NAME, value.f0.toString()));
            }
        } else {
            metas[2] = -1;
        }

        List<RowType.RowField> nonInternalFields = rowFields.stream()
                .filter(f -> !META_FIELD_NAMES.contains(f.getName())).collect(Collectors.toList());

        /*if (nonInternalFields.size() == 1) {
            String fieldName = nonInternalFields.get(0).getName();
            int fieldIndex = rowType.getFieldIndex(fieldName);
            LogicalType logicalType = rowType.getTypeAt(fieldIndex);
            valueType = TypeConversions.fromLogicalToDataType(logicalType);
            isDegradation = true;
        } else {*/
        List<DataTypes.Field> fields = nonInternalFields.stream()
                .map(f -> {
                    String fieldName = f.getName();
                    int fieldIndex = rowType.getFieldIndex(fieldName);
                    LogicalType logicalType = rowType.getTypeAt(fieldIndex);
                    return DataTypes.FIELD(fieldName, TypeConversions.fromLogicalToDataType(logicalType));
                }).collect(Collectors.toList());
        valueType = DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
        //}

        List<Integer> values = nonInternalFields.stream()
                .map(f -> name2Type.get(f.getName()).f1).collect(Collectors.toList());

        metaProjection = row -> {
            Row result = new Row(3);
            for (int i = 0; i < metas.length; i++) {
                if (metas[i] != -1) {
                    result.setField(i, row.getField(metas[i]));
                }
            }
            return result;
        };

        valueProjection = row -> {
            Row result = new Row(values.size());
            for (int i = 0; i < values.size(); i++) {
                result.setField(i, row.getField(values.get(i)));
            }
            return result;
        };
    }

    protected Schema<?> getPulsarSchema() {
        return dataType2PulsarSchema(valueType);
    }

    private Schema dataType2PulsarSchema(DataType dataType) {
        org.apache.avro.Schema avroSchema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        byte[] schemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
        SchemaInfo si = new SchemaInfo();
        si.setSchema(schemaBytes);
        // for now we just support avro and json serializationSchema
        String formatName = properties.getProperty(FormatDescriptorValidator.FORMAT_TYPE, JsonFormatFactory.IDENTIFIER);
        if (formatName.equals(AvroFormatFactory.IDENTIFIER)) {
            si.setName("Avro");
            si.setType(SchemaType.AVRO);
        } else {
            si.setName("Json");
            si.setType(SchemaType.JSON);
        }
        return Schema.generic(si);
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        checkErroneous();
        initializeSendCallback();

        Row metaRow = metaProjection.apply(value);
        Row valueRow = valueProjection.apply(value);
        byte[] serializeValue = serializationSchema.serialize(valueRow);

        String topic;
        if (forcedTopic) {
            topic = defaultTopic;
        } else {
            topic = (String) metaRow.getField(0);
        }

        String key = (String) metaRow.getField(1);
        java.sql.Timestamp eventTime = (java.sql.Timestamp) metaRow.getField(2);

        if (topic == null) {
            if (failOnWrite) {
                throw new NullPointerException("null topic present in the data");
            }
            return;
        }

        TypedMessageBuilder builder = getProducer(topic).newMessage().value(serializeValue);

        if (key != null) {
            builder.keyBytes(key.getBytes());
        }

        if (eventTime != null) {
            long et = DateTimeUtils.fromJavaTimestamp(eventTime);
            if (et > 0) {
                builder.eventTime(et);
            }
        }

        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }
        builder.sendAsync().whenComplete(sendCallback);
    }

    /**
     * Use this class for compatibility, but we do not recommend you to use {@link FlinkPulsarRowSink}.
     */
    static class RowSinkSerializationSchema extends PulsarRowSerializationSchema {
        private SerializableFunction<Row, Row> valueProjection;
        private SerializationSchema<Row> valueSerialization;

        //private SerializableFunction<Row, Row> metaProjection;
        RowSinkSerializationSchema(String topic,
                                   SerializationSchema<Row> valueSerialization,
                                   RecordSchemaType recordSchemaType,
                                   DataType dataType) {
            super(topic,
                    valueSerialization,
                    false,
                    null,
                    null,
                    recordSchemaType,
                    dataType);
            this.valueSerialization = valueSerialization;
        }

        public void setValueProjection(SerializableFunction<Row, Row> valueProjection) {
            this.valueProjection = valueProjection;
        }

        @Override
        public byte[] serialize(Row element) {
            checkNotNull(valueProjection, "valueProjection must be not null");
            Row value = valueProjection.apply(element);
            return valueSerialization.serialize(value);
        }
    }
}
