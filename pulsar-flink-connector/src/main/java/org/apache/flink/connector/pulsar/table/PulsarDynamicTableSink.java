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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.SupportsWritingMetadata;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * pulsar dynamic table sink.
 */
public class PulsarDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Metadata that is appended at the end of a physical sink row. */
    protected List<String> metadataKeys;

    /**
     *  Data type to configure the format.
     */
    protected DataType physicalDataType;

    /**
     * The pulsar topic to write to.
     */
    protected final String topic;
    protected final String serviceUrl;
    protected final String adminUrl;

    /**
     * Properties for the pulsar producer.
     */
    protected final Properties properties;

    protected final boolean useExtendField;

    /**
     * Sink format for encoding records to pulsar.
     */
    protected final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    protected PulsarDynamicTableSink(
            String serviceUrl,
            String adminUrl,
            String topic,
            DataType physicalDataType,
            Properties properties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        this.serviceUrl = Preconditions.checkNotNull(serviceUrl, "serviceUrl data type must not be null.");
        this.adminUrl = Preconditions.checkNotNull(adminUrl, "adminUrl data type must not be null.");
        this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Consumed data type must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.encodingFormat = Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null.");
        this.metadataKeys = Collections.emptyList();
        this.useExtendField = Boolean.parseBoolean(properties.getProperty(PulsarOptions.USE_EXTEND_FIELD, "false"));
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                this.encodingFormat.createRuntimeEncoder(context, this.physicalDataType);

        //if we update to FLink 1.12, planner will auto inject metadataKeys and call applyWritableMetadata method.
        if (useExtendField) {
            metadataKeys = Arrays.stream(WritableMetadata.values()).map(x -> x.key).collect(Collectors.toList());
            applyWritableMetadata(metadataKeys, null);
        }

        final SinkFunction<RowData> pulsarSink = createPulsarSink(
                this.topic,
                this.properties,
                serializationSchema);

        return SinkFunctionProvider.of(pulsarSink);
    }

    private SinkFunction<RowData> createPulsarSink(String topic, Properties properties,
                                                   SerializationSchema<RowData> valueSerialization) {
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RowData.FieldGetter[] physicalFieldGetters = IntStream.range(0, physicalChildren.size())
                .mapToObj(pos -> RowData.createFieldGetter(physicalChildren.get(pos), pos))
                .toArray(RowData.FieldGetter[]::new);

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

        //get recordSchemaType to generate row schema for pulsar.
        String formatName = properties.getProperty(FormatDescriptorValidator.FORMAT);
        RecordSchemaType recordSchemaType = Enum.valueOf(RecordSchemaType.class, formatName.toUpperCase());

        final DynamicPulsarSerializationSchema serializationSchema = new DynamicPulsarSerializationSchema(
                topic,
                valueSerialization,
                metadataKeys.size() > 0,
                metadataPositions,
                physicalFieldGetters,
                recordSchemaType,
                physicalDataType);

        return new FlinkPulsarSink<RowData>(
                serviceUrl,
                adminUrl,
                Optional.ofNullable(topic),
                properties,
                serializationSchema
        );
    }

    @Override
    public DynamicTableSink copy() {
        final PulsarDynamicTableSink copy = new PulsarDynamicTableSink(
                this.serviceUrl,
                this.adminUrl,
                this.topic,
                this.physicalDataType,
                this.properties,
                this.encodingFormat
        );
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Pulsar universal table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarDynamicTableSink that = (PulsarDynamicTableSink) o;
        return useExtendField == that.useExtendField &&
                Objects.equals(metadataKeys, that.metadataKeys) &&
                Objects.equals(physicalDataType, that.physicalDataType) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(serviceUrl, that.serviceUrl) &&
                Objects.equals(adminUrl, that.adminUrl) &&
                //Objects.equals(properties, that.properties) &&
                Objects.equals(encodingFormat, that.encodingFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataKeys, physicalDataType, topic, serviceUrl, adminUrl, properties, useExtendField, encodingFormat);
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
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    return row.getTimestamp(pos, 3).getMillisecond();
                });

        public final String key;

        public final DataType dataType;

        public final DynamicPulsarSerializationSchema.WritableRowDataMetadataConverter converter;

        WritableMetadata(String key, DataType dataType, DynamicPulsarSerializationSchema.WritableRowDataMetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
