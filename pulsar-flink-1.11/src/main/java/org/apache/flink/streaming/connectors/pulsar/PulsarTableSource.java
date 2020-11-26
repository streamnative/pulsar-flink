/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.table.DynamicPulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.table.PulsarDynamicTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.EVENT_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.KEY_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.MESSAGE_ID_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PUBLISH_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Pulsar {@StreamTableSource}.
 */
@Slf4j
@EqualsAndHashCode
public class PulsarTableSource
        implements StreamTableSource<Row>,
        DefinedProctimeAttribute,
        DefinedRowtimeAttributes,
        DefinedFieldMapping,
        SupportsReadingMetadata {

    private final String serviceUrl;
    private final String adminUrl;

    /** The startup mode for the contained consumer (default is {@link StartupMode#LATEST}). */
    private final StartupMode startupMode;

    private final Map<String, MessageId> specificStartupOffsets;

    private final String externalSubscriptionName;

    private final Map<String, String> caseInsensitiveParams;

    private final Optional<TableSchema> providedSchema;
    private final Optional<String> proctimeAttribute;
    private final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;
    private final Properties properties;

    private final TableSchema schema;

    private final Optional<Map<String, String>> fieldMapping;
    private final DeserializationSchema<Row> deserializationSchema;

    private final SchemaTranslator schemaTranslator;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    protected final boolean useExtendField;

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Data type to configure the format. */
    protected final DataType physicalDataType;

    protected TypeInformation<Row> producedTypeInfo;

    public PulsarTableSource(
            Optional<TableSchema> providedSchema,
            Optional<String> proctimeAttribute,
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
            Optional<Map<String, String>> fieldMapping,
            String serviceUrl,
            String adminUrl,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            StartupMode startupMode,
            Map<String, MessageId> specificStartupOffsets,
            String externalSubscriptionName) {

        this.providedSchema = providedSchema;
        this.serviceUrl = checkNotNull(serviceUrl);
        this.adminUrl = checkNotNull(adminUrl);
        this.properties = checkNotNull(properties);
        this.startupMode = startupMode;
        this.specificStartupOffsets = specificStartupOffsets;
        this.externalSubscriptionName = externalSubscriptionName;

        this.caseInsensitiveParams =
                SourceSinkUtils.validateStreamSourceOptions(Maps.fromProperties(properties));
        this.schemaTranslator = new SimpleSchemaTranslator();
        this.schema = inferTableSchema();

        this.proctimeAttribute = validateProctimeAttribute(proctimeAttribute);
        this.rowtimeAttributeDescriptors = validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);
        this.fieldMapping = fieldMapping;
        this.deserializationSchema = deserializationSchema;
        this.metadataKeys = Collections.emptyList();
        this.physicalDataType = schema.toRowDataType();
        this.useExtendField = Boolean.parseBoolean(
                properties.getProperty(
                        PulsarOptions.USE_EXTEND_FIELD, "false"
                ));
    }

    public PulsarTableSource(
            String serviceUrl,
            String adminUrl,
            Properties properties) {
        this(
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty(),
                serviceUrl,
                adminUrl,
                properties,
                null,
                StartupMode.LATEST,
                Collections.emptyMap(),
                null);
    }

    @Override
    public String getProctimeAttribute() {
        return proctimeAttribute.orElse(null);
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return rowtimeAttributeDescriptors;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        if(useExtendField) return producedTypeInfo;
        else return deserializationSchema.getProducedType();
    }

    /*  @Override
      public DataType getProducedDataType() {
          if (getDeserializationSchema() != null) {
              TypeInformation<Row> legacyType = deserializationSchema.getProducedType();
              return TypeConversions.fromLegacyInfoToDataType(legacyType);
          }
          return schema.toRowDataType();
      }
  */
    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public Map<String, String> getFieldMapping() {
        return fieldMapping.orElse(null);
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {

        //if we update to FLink 1.12, planner will auto inject metadataKeys and call applyReadableMetadata method.
        if (useExtendField) {
            metadataKeys = Arrays.stream(ReadableMetadata.values()).map(x -> x.key).collect(Collectors.toList());
            applyReadableMetadata(metadataKeys, generateProducedDataType());
            producedTypeInfo = (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(producedDataType);
        }

        final PulsarRowDeserializationSchema.ReadableRowMetadataConverter[] metadataConverters = metadataKeys.stream()
                .map(k ->
                        Stream.of(ReadableMetadata.values())
                                .filter(rm -> rm.key.equals(k))
                                .findFirst()
                                .orElseThrow(IllegalStateException::new))
                .map(m -> m.converter)
                .toArray(PulsarRowDeserializationSchema.ReadableRowMetadataConverter[]::new);

        PulsarRowDeserializationSchema pulsarDeserializationSchema =
                new PulsarRowDeserializationSchema(deserializationSchema, metadataConverters.length > 0, metadataConverters, producedTypeInfo);
        //PulsarDeserializationSchema<Row> deserializer = getDeserializationSchema();
        FlinkPulsarSource<Row> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, pulsarDeserializationSchema, properties);
        //FlinkPulsarRowSource source = new FlinkPulsarRowSource(serviceUrl, adminUrl, properties, deserializer);
        switch (startupMode) {
            case EARLIEST:
                source.setStartFromEarliest();
                break;
            case LATEST:
                source.setStartFromLatest();
                break;
            case SPECIFIC_OFFSETS:
                source.setStartFromSpecificOffsets(specificStartupOffsets);
                break;
            case EXTERNAL_SUBSCRIPTION:
                MessageId subscriptionPosition = MessageId.latest;
                if (CONNECTOR_STARTUP_MODE_VALUE_EARLIEST.equals(properties.get(CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET))) {
                    subscriptionPosition = MessageId.earliest;
                }
                source.setStartFromSubscription(externalSubscriptionName, subscriptionPosition);
        }

        return execEnv.addSource(source).name(explainSource());
    }

    private TableSchema inferTableSchema() {
        if (providedSchema.isPresent()) {
            return providedSchema.get();
        } else {
            try {
                PulsarMetadataReader reader = new PulsarMetadataReader(adminUrl, new ClientConfigurationData(), "", caseInsensitiveParams, -1, -1);
                List<String> topics = reader.getTopics()
                        .stream()
                        .map(TopicRange::getTopic)
                        .collect(Collectors.toList());
                SchemaInfo pulsarSchema = reader.getPulsarSchema(topics);
                return schemaTranslator.pulsarSchemaToTableSchema(pulsarSchema);
            } catch (PulsarClientException | PulsarAdminException | IncompatibleSchemaException e) {
                log.error("Failed to fetch table schema", adminUrl);
                throw new RuntimeException(e);
            }
        }
    }

    //////// VALIDATION FOR PARAMETERS

    /**
     * Validates a field of the schema to be the processing time attribute.
     *
     * @param proctimeAttribute The name of the field that becomes the processing time field.
     */
    private Optional<String> validateProctimeAttribute(Optional<String> proctimeAttribute) {
        return proctimeAttribute.map((attribute) -> {
            // validate that field exists and is of correct type
            Optional<TypeInformation<?>> tpe = schema.getFieldType(attribute);
            if (!tpe.isPresent()) {
                throw new ValidationException("Processing time attribute '" + attribute + "' is not present in TableSchema.");
            } else if (tpe.get() != Types.SQL_TIMESTAMP()) {
                throw new ValidationException("Processing time attribute '" + attribute + "' is not of type SQL_TIMESTAMP.");
            }
            return attribute;
        });
    }

    /**
     * Validates a list of fields to be rowtime attributes.
     *
     * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
     */
    private List<RowtimeAttributeDescriptor> validateRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
        Preconditions.checkNotNull(rowtimeAttributeDescriptors, "List of rowtime attributes must not be null.");
        // validate that all declared fields exist and are of correct type
        for (RowtimeAttributeDescriptor desc : rowtimeAttributeDescriptors) {
            String rowtimeAttribute = desc.getAttributeName();
            Optional<TypeInformation<?>> tpe = schema.getFieldType(rowtimeAttribute);
            if (!tpe.isPresent()) {
                throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not present in TableSchema.");
            } else if (tpe.get() != Types.SQL_TIMESTAMP()) {
                throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not of type SQL_TIMESTAMP.");
            }
        }
        return rowtimeAttributeDescriptors;
    }

    public PulsarDeserializationSchema<Row> getDeserializationSchema() {
        if (deserializationSchema == null) {
            throw new RuntimeException("in table mode, deserializationSchema is needed.");
        }
        return new PulsarDeserializationSchemaWrapper<>(deserializationSchema);
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

    public enum ReadableMetadata {
        KEY_ATTRIBUTE(
                KEY_ATTRIBUTE_NAME,
                DataTypes.BYTES().nullable(),
                record -> record.getKeyBytes()
        ),

        TOPIC_ATTRIBUTE(
                TOPIC_ATTRIBUTE_NAME,
                DataTypes.STRING().notNull(),
                record -> record.getTopicName()
        ),

        MESSAGE_ID(
                MESSAGE_ID_NAME,
                DataTypes.BYTES().nullable(),
                record -> record.getMessageId().toByteArray()),

        PUBLISH_TIME(
                PUBLISH_TIME_NAME,
                DataTypes.TIMESTAMP(3).nullable(),
                record -> LocalDateTime.ofInstant(Instant.ofEpochMilli(record.getPublishTime()), ZoneId.systemDefault())),

        EVENT_TIME(
                EVENT_TIME_NAME,
                DataTypes.TIMESTAMP(3).nullable(),
                record -> LocalDateTime.ofInstant(Instant.ofEpochMilli(record.getEventTime()), ZoneId.systemDefault()));

        public final String key;

        public final DataType dataType;

        public final PulsarRowDeserializationSchema.ReadableRowMetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, PulsarRowDeserializationSchema.ReadableRowMetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
