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

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.pulsar.internal.DateTimeUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializer;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.META_FIELD_NAMES;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.KEY_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.EVENT_TIME_NAME;

@Slf4j
public class FlinkPulsarRowSink extends FlinkPulsarSinkBase<Row> {

    protected DataType dataType;

    private DataType valueType;

    private Function<Row, Row> valueProjection;

    private Function<Row, Row> metaProjection;

    private PulsarSerializer serializer;

    private boolean valueIsStruct;

    public FlinkPulsarRowSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            DataType dataType) {
       super(
           adminUrl,
           defaultTopicName,
           clientConf,
           properties,
           TopicKeyExtractor.DUMMY_FOR_ROW);

       dataType = this.dataType;
    }

    public FlinkPulsarRowSink(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            DataType dataType) {
        this(adminUrl, defaultTopicName, newClientConf(serviceUrl), properties, dataType);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        createProjection();
        this.serializer = new PulsarSerializer(valueType, false);
    }

    private void createProjection() {
        val metas = new int[3];

        val fdt = (FieldsDataType) dataType;
        val fdtm = fdt.getFieldDataTypes();

        val rowFields = ((RowType) fdt.getLogicalType()).getFields();
        val name2Type = new HashMap<String, Tuple2<LogicalTypeRoot, Integer>>();
        for (int i = 0; i < rowFields.size(); i++) {
            val rf = rowFields.get(i);
            name2Type.put(rf.getName(), new Tuple2<>(rf.getType().getTypeRoot(), i));
        }

        // topic
        if (name2Type.containsKey(TOPIC_ATTRIBUTE_NAME)) {
            val value = name2Type.get(TOPIC_ATTRIBUTE_NAME);
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
            val value = name2Type.get(KEY_ATTRIBUTE_NAME);
            if (value.f0 == LogicalTypeRoot.VARBINARY) {
                metas[0] = value.f1;
            } else {
                throw new IllegalStateException(
                    String.format("%s attribute unsupported type %s", KEY_ATTRIBUTE_NAME, value.f0.toString()));
            }
        } else {
            metas[1] = -1;
        }

        // eventTime
        if (name2Type.containsKey(EVENT_TIME_NAME)) {
            val value = name2Type.get(EVENT_TIME_NAME);
            if (value.f0 == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                metas[0] = value.f1;
            } else {
                throw new IllegalStateException(
                    String.format("%s attribute unsupported type %s", EVENT_TIME_NAME, value.f0.toString()));
            }
        } else {
            metas[2] = -1;
        }

        val nonInternalFields = rowFields.stream()
            .filter(f -> !META_FIELD_NAMES.contains(f.getName())).collect(Collectors.toList());

        if (nonInternalFields.size() == 1) {
            val fieldName = nonInternalFields.get(0).getName();
            valueType = fdtm.get(fieldName);
        } else {
            val fields = nonInternalFields.stream()
                .map(f -> {
                    val fieldName = f.getName();
                    return DataTypes.FIELD(fieldName, fdtm.get(fieldName));
                }).collect(Collectors.toList());
            valueType = DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
        }

        val values = nonInternalFields.stream()
            .map(f -> name2Type.get(f.getName()).f1).collect(Collectors.toList());

        metaProjection = row -> {
            val result = new Row(3);
            for (int i = 0; i < metas.length; i++) {
                if (metas[i] != -1) {
                    result.setField(i, row.getField(metas[i]));
                }
            }
            return result;
        };

        valueProjection = row -> {
            val result = new Row(values.size());
            for (int i = 0; i < values.size(); i++) {
                result.setField(i, row.getField(values.get(i)));
            }
            return result;
        };

        valueIsStruct = nonInternalFields.size() == 1;
    }

    @Override
    protected Schema<?> getPulsarSchema() {
        try {
            return SchemaUtils.sqlType2PulsarSchema(valueType);
        } catch (SchemaUtils.IncompatibleSchemaException e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        checkErroneous();

        val metaRow = metaProjection.apply(value);
        val valueRow = valueProjection.apply(value);
        val v = serializer.serialize(valueRow);

        String topic;
        if (forcedTopic) {
            topic = defaultTopic;
        } else {
            topic = (String) metaRow.getField(0);
        }

        val key = (String) metaRow.getField(1);
        val eventTime = (java.sql.Timestamp) metaRow.getField(2);

        if (topic == null) {
            if (failOnWrite) {
                throw new NullPointerException("null topic present in the data");
            }
            return;
        }

        val builder = getProducer(topic).newMessage().value((Row) v);

        if (key != null) {
            builder.keyBytes(key.getBytes());
        }

        if (eventTime != null) {
            val et = DateTimeUtils.fromJavaTimestamp(eventTime);
            if (et > 0) {
                builder.eventTime(et);
            }
        }

        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords ++;
            }
        }
        builder.sendAsync().whenComplete(sendCallback);
    }
}
