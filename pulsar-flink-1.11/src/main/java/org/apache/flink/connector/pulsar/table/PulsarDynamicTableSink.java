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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.TopicKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
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

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;
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

    /**
     * Sink format for encoding records to pulsar.
     */
    protected final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /** Sink commit semantic. */
    protected final PulsarSinkSemantic semantic;

    protected PulsarDynamicTableSink(
            String serviceUrl,
            String adminUrl,
            String topic,
            DataType physicalDataType,
            Properties properties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            PulsarSinkSemantic semantic) {
        this.serviceUrl = Preconditions.checkNotNull(serviceUrl, "serviceUrl data type must not be null.");
        this.adminUrl = Preconditions.checkNotNull(adminUrl, "adminUrl data type must not be null.");
        this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Consumed data type must not be null.");
        // Mutable attributes
        this.metadataKeys = Collections.emptyList();
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.encodingFormat = Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null.");
        this.semantic = Preconditions.checkNotNull(semantic, "Semantic must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
//       must valueProjection type is int[]
        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, encodingFormat, new int[]{}, null);

        final SinkFunction<RowData> pulsarSink = createPulsarSink(
                this.topic,
                this.properties,
                valueSerialization);

        return SinkFunctionProvider.of(pulsarSink);
    }

    private SinkFunction<RowData> createPulsarSink(String topic, Properties properties,
                                                   SerializationSchema<RowData> serializationSchema) {
        switch (semantic){
            case AT_LEAST_ONCE:
            case NONE:
                return new FlinkPulsarSink<RowData>(
                        serviceUrl,
                        adminUrl,
                        Optional.ofNullable(topic),
                        properties,
                        TopicKeyExtractor.NULL,
                        RowData.class,
                        RecordSchemaType.AVRO
                );
            case EXACTLY_ONCE:
            default:
                throw new IllegalArgumentException("not support sink semantic: " + semantic);
        }

    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    @Override
    public DynamicTableSink copy() {
        return new PulsarDynamicTableSink(
                this.serviceUrl,
                this.adminUrl,
                this.topic,
                this.physicalDataType,
                this.properties,
                this.encodingFormat,
                this.semantic);
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
        if (!(o instanceof PulsarDynamicTableSink)) {
            return false;
        }
        PulsarDynamicTableSink that = (PulsarDynamicTableSink) o;
        return physicalDataType.equals(that.physicalDataType) &&
                Objects.equals(topic, that.topic) &&
                serviceUrl.equals(that.serviceUrl) &&
                adminUrl.equals(that.adminUrl) &&
                encodingFormat.equals(that.encodingFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalDataType, topic, serviceUrl, adminUrl, encodingFormat);
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

        PROPERTIES(
                "properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable(),
                (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    final MapData map = row.getMap(pos);
                    final ArrayData keyArray = map.keyArray();
                    final ArrayData valueArray = map.valueArray();

                    final Properties properties = new Properties(keyArray.size());
                    for (int i = 0; i < keyArray.size(); i++) {
                        if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                            final String key = keyArray.getString(i).toString();
                            final String value = valueArray.getString(i).toString();
                            properties.put(key,value);
                        }
                    }
                    return properties;
                }
        ),

        EVENT_TIME(
                "eventTime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    return row.getTimestamp(pos, 3).getMillisecond();
                }),
        KEY(
                "key",
                DataTypes.STRING().nullable(),
                (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    return row.getString(pos).toString();
                });

        final String key;

        final DataType dataType;

        final DynamicPulsarSerializationSchema.MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, DynamicPulsarSerializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }


}
