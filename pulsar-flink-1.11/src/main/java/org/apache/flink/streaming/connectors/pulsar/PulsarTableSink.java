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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonSer;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import lombok.EqualsAndHashCode;
import org.apache.commons.lang.SerializationUtils;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Pulsar Table Sink.
 */
@EqualsAndHashCode
public class PulsarTableSink implements AppendStreamTableSink<Row>, SupportsWritingMetadata {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Metadata that is appended at the end of a physical sink row. */
    protected List<String> metadataKeys;

    /**
     *  Data type to configure the format.
     */
    protected DataType physicalDataType;

    protected final boolean useExtendField;

    private final String adminUrl;

    private final TableSchema schema;

    private final String defaultTopicName;

    private final ClientConfigurationData clientConf;

    private final Properties properties;

    private SerializationSchema serializationSchema;

    public PulsarTableSink(String adminUrl,
                           TableSchema schema,
                           String defaultTopicName,
                           ClientConfigurationData clientConf,
                           Properties properties,
                           SerializationSchema serializationSchema
                           ) {
        this.adminUrl = checkNotNull(adminUrl);
        this.schema = checkNotNull(schema);
        this.defaultTopicName = defaultTopicName;
        this.clientConf = checkNotNull(clientConf);
        this.properties = checkNotNull(properties);
        this.serializationSchema = serializationSchema;
        this.physicalDataType = schema.toRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.useExtendField = Boolean.parseBoolean(
                properties.getProperty(
                        PulsarOptions.USE_EXTEND_FIELD, "false"
                ));
    }

    public PulsarTableSink(String serviceUrl, String adminUrl, TableSchema schema, String defaultTopicName, Properties properties, SerializationSchema serializationSchema) {
        this(adminUrl, schema, defaultTopicName, PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties), properties, serializationSchema);
    }

    public void emitDataStream(DataStream<Row> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {

        //if we update to FLink 1.12, planner will auto inject metadataKeys and call applyWritableMetadata method.
        if(useExtendField){
            metadataKeys = Arrays.stream(WritableMetadata.values()).map(x -> x.key).collect(Collectors.toList());
            applyWritableMetadata(metadataKeys, null);
        }
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        //get recordSchemaType to generate row schema for pulsar.
        String formatName = properties.getProperty(FormatDescriptorValidator.FORMAT_TYPE);
        final int[] physicalPos = IntStream.range(0, physicalChildren.size()).toArray();
        RecordSchemaType recordSchemaType = Enum.valueOf(RecordSchemaType.class, formatName.toUpperCase());

        // determine the positions of metadata in the sink row
        final int[] metadataPositions = Stream.of(WritableMetadata.values())
                .mapToInt(m -> {
                    final int pos = metadataKeys.indexOf(m.key);
                    if (pos < 0) {
                        return -1;
                    }
                    return physicalChildren.size() + pos;
                })
                .toArray();
        PulsarRowSerializationSchema pulsarRowSerializationSchema = new PulsarRowSerializationSchema(
                defaultTopicName,
                serializationSchema,
                metadataKeys.size() > 0,
                metadataPositions,
                physicalPos,
                recordSchemaType,
                physicalDataType);
        FlinkPulsarSink<Row> sink = new FlinkPulsarSink<>(adminUrl, Optional.of(defaultTopicName), clientConf, properties, pulsarRowSerializationSchema);
        //FlinkPulsarRowSink sink = new FlinkPulsarRowSink(adminUrl, defaultTopicName, clientConf, properties, serializationSchema, schema.toRowDataType());
        return dataStream
                .addSink(sink)
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(getClass(), getFieldNames()));
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return schema.toRowType();
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(WritableMetadata.values()).forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
    }

    enum WritableMetadata {

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                (row, pos) -> {
                    if (row.getField(pos) == null) {
                        return null;
                    }
                    return ((TimestampData)row.getField(pos)).getMillisecond();
                });

        public final String key;

        public final DataType dataType;

        public final PulsarRowSerializationSchema.WritableRowMetadataConverter converter;

        WritableMetadata(String key, DataType dataType, PulsarRowSerializationSchema.WritableRowMetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
