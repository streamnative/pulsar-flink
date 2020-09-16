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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Pulsar {@StreamTableSource}.
 */
@Slf4j
@EqualsAndHashCode
public class PulsarTableSource
        implements StreamTableSource<Row>,
        DefinedProctimeAttribute,
        DefinedRowtimeAttributes {

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

    private final SchemaTranslator schemaTranslator;

    public PulsarTableSource(
            Optional<TableSchema> providedSchema,
            Optional<String> proctimeAttribute,
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
            String serviceUrl,
            String adminUrl,
            Properties properties,
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
        this.schemaTranslator = new SimpleSchemaTranslator(true);
        this.schema = inferTableSchema();

        this.proctimeAttribute = validateProctimeAttribute(proctimeAttribute);
        this.rowtimeAttributeDescriptors = validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);
    }

    public PulsarTableSource(
            String serviceUrl,
            String adminUrl,
            Properties properties) {
        this(
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                serviceUrl,
                adminUrl,
                properties,
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
    public DataType getProducedDataType() {
        return schema.toRowDataType();
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        FlinkPulsarRowSource source = new FlinkPulsarRowSource(serviceUrl, adminUrl, properties);
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
                source.setStartFromSubscription(externalSubscriptionName);
        }

        return execEnv.addSource(source).name(explainSource());
    }

    private TableSchema inferTableSchema() {
        if (providedSchema.isPresent()) {
            return providedSchema.get();
        } else {
            try {
                PulsarMetadataReader reader = new PulsarMetadataReader(adminUrl, new ClientConfigurationData(), "", caseInsensitiveParams, -1, -1);
                List<String> topics = reader.getTopics();
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

}
