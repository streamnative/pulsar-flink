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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.SupportsReadingMetadata;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.EVENT_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.KEY_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.MESSAGE_ID_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PUBLISH_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;

/**
 * pulsar dynamic table source.
 */
@Slf4j
public class PulsarDynamicTableSource implements ScanTableSource, SupportsReadingMetadata {

    // --------------------------------------------------------------------------------------------
    // Common attributes
    // --------------------------------------------------------------------------------------------
    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Data type to configure the format. */
    protected final DataType physicalDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    // --------------------------------------------------------------------------------------------
    // Scan format attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Scan format for decoding records from Pulsar.
     */
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

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

    protected final boolean useExtendField;

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
        this.physicalDataType = physicalDataType;
        this.decodingFormat = decodingFormat;
        this.producedDataType = physicalDataType;
        this.topics = topics;
        this.topicPattern = topicPattern;
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
        setTopicInfo(properties, topics, topicPattern);
        this.properties = properties;
        this.startupOptions = startupOptions;
        this.metadataKeys = Collections.emptyList();
        this.useExtendField = Boolean.parseBoolean(
                properties.getProperty(
                        org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.USE_EXTEND_FIELD, "false"
                ));
    }

    private void setTopicInfo(Properties properties, List<String> topics, String topicPattern) {
        if (StringUtils.isNotBlank(topicPattern)) {
            properties.putIfAbsent("topicspattern", topicPattern);
            properties.remove("topic");
            properties.remove("topics");
        } else if (topics != null && topics.size() > 1) {
            properties.putIfAbsent("topics", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topic");
        } else if (topics != null && topics.size() == 1) {
            properties.putIfAbsent("topic", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topics");
        } else {
            throw new RuntimeException("Use `topics` instead of `topic` for multi topic read");
        }
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        DeserializationSchema<RowData> deserializationSchema =
                this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalDataType);

        TypeInformation<RowData> producedTypeInfo = null;
        //if we update to FLink 1.12, planner will auto inject metadataKeys and call applyReadableMetadata method.
        if (useExtendField) {
            metadataKeys = Arrays.stream(ReadableMetadata.values()).map(x -> x.key).collect(Collectors.toList());
            applyReadableMetadata(metadataKeys, generateProducedDataType());
            producedTypeInfo = (TypeInformation<RowData>) runtimeProviderContext.createTypeInformation(producedDataType);
        }

        final DynamicPulsarDeserializationSchema.ReadableRowDataMetadataConverter[] metadataConverters = metadataKeys.stream()
                .map(k ->
                        Stream.of(ReadableMetadata.values())
                                .filter(rm -> rm.key.equals(k))
                                .findFirst()
                                .orElseThrow(IllegalStateException::new))
                .map(m -> m.converter)
                .toArray(DynamicPulsarDeserializationSchema.ReadableRowDataMetadataConverter[]::new);

        FlinkPulsarSource<RowData> source = new FlinkPulsarSource<RowData>(
                adminUrl,
                newClientConf(serviceUrl),
                new DynamicPulsarDeserializationSchema(
                        deserializationSchema,
                        metadataKeys.size() > 0,
                        metadataConverters,
                        producedTypeInfo
                ),
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

    private DataType generateProducedDataType() {
        List<DataTypes.Field> mainSchema = new ArrayList<>();
        if (physicalDataType instanceof FieldsDataType) {
            FieldsDataType fieldsDataType = (FieldsDataType) physicalDataType;
            RowType rowType = (RowType) fieldsDataType.getLogicalType();
            List<String> fieldNames = rowType.getFieldNames();
            for (int i = 0; i < fieldNames.size(); i++) {
                org.apache.flink.table.types.logical.LogicalType logicalType = rowType.getTypeAt(i);
                DataTypes.Field field = DataTypes.FIELD(fieldNames.get(i), TypeConversions.fromLogicalToDataType(logicalType));
                mainSchema.add(field);
            }
        } else {
            mainSchema.add(DataTypes.FIELD("value", physicalDataType));
        }
        if (useExtendField) {
            mainSchema.addAll(SimpleSchemaTranslator.METADATA_FIELDS);
        }
        FieldsDataType fieldsDataType = (FieldsDataType) DataTypes.ROW(mainSchema.toArray(new DataTypes.Field[0]));
        return fieldsDataType;
    }

    @Override
    public DynamicTableSource copy() {
        final PulsarDynamicTableSource copy = new PulsarDynamicTableSource(
                this.producedDataType,
                this.decodingFormat,
                this.topics,
                this.topicPattern,
                this.serviceUrl,
                this.adminUrl,
                this.properties,
                this.startupOptions
        );
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        return copy;
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarDynamicTableSource that = (PulsarDynamicTableSource) o;
        return useExtendField == that.useExtendField &&
                Objects.equals(producedDataType, that.producedDataType) &&
                Objects.equals(physicalDataType, that.physicalDataType) &&
                Objects.equals(metadataKeys, that.metadataKeys) &&
                Objects.equals(decodingFormat, that.decodingFormat) &&
                Objects.equals(topics, that.topics) &&
                Objects.equals(topicPattern, that.topicPattern) &&
                Objects.equals(serviceUrl, that.serviceUrl) &&
                Objects.equals(adminUrl, that.adminUrl) &&
                //Objects.equals(properties, that.properties) &&
                Objects.equals(startupOptions, that.startupOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producedDataType, physicalDataType, metadataKeys, decodingFormat, topics, topicPattern, serviceUrl, useExtendField, adminUrl, properties, startupOptions);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(ReadableMetadata.values()).forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    /**
     * readableMetadata for new table api.
     */
    public enum ReadableMetadata {
        KEY_ATTRIBUTE(
                KEY_ATTRIBUTE_NAME,
                DataTypes.BYTES().nullable(),
                record -> record.getKeyBytes()
        ),

        TOPIC_ATTRIBUTE(
                TOPIC_ATTRIBUTE_NAME,
                DataTypes.STRING().nullable(),
                record -> StringData.fromString(record.getTopicName())
        ),

        MESSAGE_ID(
                MESSAGE_ID_NAME,
                DataTypes.BYTES().nullable(),
                record -> record.getMessageId().toByteArray()),

        EVENT_TIME(
                EVENT_TIME_NAME,
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                record -> TimestampData.fromEpochMillis(record.getEventTime())),

        PUBLISH_TIME(
                PUBLISH_TIME_NAME,
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                record -> TimestampData.fromEpochMillis(record.getPublishTime()));

        public final String key;

        public final DataType dataType;

        public final DynamicPulsarDeserializationSchema.ReadableRowDataMetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, DynamicPulsarDeserializationSchema.ReadableRowDataMetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

}
