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

package org.apache.flink.streaming.connectors.pulsar.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;

/**
 * pulsar dynamic table source.
 */
@Slf4j
public class PulsarDynamicTableSource implements ScanTableSource, SupportsReadingMetadata {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String VALUE_METADATA_PREFIX = "value.";

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /**
     * Scan format for decoding records from Pulsar.
     */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    // --------------------------------------------------------------------------------------------
    // Pulsar-specific attributes
    // --------------------------------------------------------------------------------------------

    /**
     * The Pulsar topic to consume.
     */
    protected final List<String> topics;

    /**
     * The Pulsar topic to consume.
     */
    protected final String topicPattern;

    /**
     * The Pulsar topic to consume.
     */
    protected final String serviceUrl;

    /**
     * The Pulsar topic to consume.
     */
    protected final String adminUrl;

    /**
     * Properties for the Pulsar consumer.
     */
    protected final Properties properties;

    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#LATEST}).
     */
    protected final PulsarOptions.StartupOptions startupOptions;

    /**
     * The default value when startup timestamp is not used.
     */
    private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

    public PulsarDynamicTableSource(DataType physicalDataType,
                                    DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                    List<String> topics,
                                    String topicPattern,
                                    String serviceUrl,
                                    String adminUrl,
                                    Properties properties,
                                    PulsarOptions.StartupOptions startupOptions) {
        this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.producedDataType = physicalDataType;
        this.valueDecodingFormat = decodingFormat;
        this.topics = topics;
        this.topicPattern = topicPattern;
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
        setTopicInfo(properties, topics, topicPattern);
        this.properties = properties;
        this.startupOptions = startupOptions;
        // Mutable attributes
        this.metadataKeys = Collections.emptyList();
    }

    private void setTopicInfo(Properties properties, List<String> topics, String topicPattern) {
        if (StringUtils.isNotBlank(topicPattern)){
            properties.putIfAbsent("topicspattern", topicPattern);
            properties.remove("topic");
            properties.remove("topics");
        } else if (topics != null && topics.size() > 1) {
            properties.putIfAbsent("topics", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topic");
        } else if (topics != null && topics.size() == 1){
            properties.putIfAbsent("topic", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topics");
        } else {
            throw new RuntimeException("Use `topics` instead of `topic` for multi topic read");
        }
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {

        //valueProjection type int[]
        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, new int[]{}, "");

        final ClientConfigurationData clientConfigurationData = newClientConf(serviceUrl);
        FlinkPulsarSource<RowData> source = new FlinkPulsarSource<RowData>(
                adminUrl,
                clientConfigurationData,
                PulsarDeserializationSchema.valueOnly(valueDeserialization),
                properties
        );
        switch (startupOptions.startupMode) {
            case EARLIEST:
                source.setStartFromEarliest();
                break;
            case LATEST:
                source.setStartFromLatest();
                break;
            case SPECIFIC_OFFSETS:
                source.setStartFromSpecificOffsets(startupOptions.specificOffsets);
                break;
            case EXTERNAL_SUBSCRIPTION:
                MessageId subscriptionPosition = MessageId.latest;
                if (CONNECTOR_STARTUP_MODE_VALUE_EARLIEST
                        .equals(properties.get(CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET))) {
                    subscriptionPosition = MessageId.earliest;
                }
                source.setStartFromSubscription(startupOptions.externalSubscriptionName, subscriptionPosition);
        }
        return SourceFunctionProvider.of(source, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new PulsarDynamicTableSource(
                this.physicalDataType,
                this.valueDecodingFormat,
                this.topics,
                this.topicPattern,
                this.serviceUrl,
                this.adminUrl,
                this.properties,
                this.startupOptions
        );
    }

    @Override
    public String asSummaryString() {
        return "Pulsar universal table source";
    }

    private static ClientConfigurationData newClientConf(String serviceUrl) {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(serviceUrl);
        return clientConf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PulsarDynamicTableSource)) {
            return false;
        }
        PulsarDynamicTableSource that = (PulsarDynamicTableSource) o;
        return physicalDataType.equals(that.physicalDataType) &&
                valueDecodingFormat.equals(that.valueDecodingFormat) &&
                Objects.equals(topics, that.topics) &&
                Objects.equals(topicPattern, that.topicPattern) &&
                serviceUrl.equals(that.serviceUrl) &&
                adminUrl.equals(that.adminUrl) &&
                //The properties generated by flink can't be compared to your own strength for now,
                // because the content is always a bit different.
                // properties.equals(that.properties) &&
                startupOptions.equals(that.startupOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalDataType, valueDecodingFormat, topics, topicPattern, serviceUrl, adminUrl, startupOptions);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        valueDecodingFormat
                .listReadableMetadata()
                .forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));

        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // separate connector and format metadata
        final List<String> formatMetadataKeys = metadataKeys.stream()
                .filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
                .collect(Collectors.toList());
        final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
        connectorMetadataKeys.removeAll(formatMetadataKeys);

        // push down format metadata
        final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys = formatMetadataKeys.stream()
                    .map(k -> k.substring(VALUE_METADATA_PREFIX.length()))
                    .collect(Collectors.toList());
            valueDecodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
        }

        this.metadataKeys = connectorMetadataKeys;
        this.producedDataType = producedDataType;
    }

    private @Nullable
    DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {

        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                message -> StringData.fromString(message.getTopicName())
        ),

        MESSAGE_ID(
                "messageId",
                DataTypes.BYTES().notNull(),
                message -> BinaryArrayData.fromPrimitiveArray(message.getMessageId().toByteArray())),

        SEQUENCE_ID(
                "sequenceId",
                DataTypes.BIGINT().notNull(),
                Message::getSequenceId),
        KEY(
                "key",
                DataTypes.STRING().nullable(),
                message -> StringData.fromString(message.getKey())
        ),

        PUBLISH_TIME(
                "publishTime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                message -> TimestampData.fromEpochMillis(message.getPublishTime())),

        EVENT_TIME(
                "eventTime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                message -> TimestampData.fromEpochMillis(message.getEventTime())),

        PROPERTIES(
                "properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable()).notNull(),
                message -> new GenericMapData(message.getProperties())
        );

        final String key;

        final DataType dataType;

        final DynamicPulsarDeserializationSchema.MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, DynamicPulsarDeserializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
