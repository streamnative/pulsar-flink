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

package org.apache.flink.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.PulsarSchemaValidator;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchemaWrapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.PulsarValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;

/**
 * pulsar dynamic table source.
 */
public class PulsarDynamicTableSource implements ScanTableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarDynamicTableSource.class);

    // --------------------------------------------------------------------------------------------
    // Common attributes
    // --------------------------------------------------------------------------------------------
    protected final DataType outputDataType;

    // --------------------------------------------------------------------------------------------
    // Scan format attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Scan format for decoding records from Kafka.
     */
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    // --------------------------------------------------------------------------------------------
    // Kafka-specific attributes
    // --------------------------------------------------------------------------------------------

    /**
     * The Kafka topic to consume.
     */
    protected final String topic;

    /**
     * The Kafka topic to consume.
     */
    protected final String serviceUrl;

    /**
     * The Kafka topic to consume.
     */
    protected final String adminUrl;

    /**
     * Properties for the Kafka consumer.
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

    public PulsarDynamicTableSource(DataType outputDataType,
                                    DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                    String topic,
                                    String serviceUrl,
                                    String adminUrl,
                                    Properties properties,
                                    PulsarOptions.StartupOptions startupOptions) {
        this.outputDataType = outputDataType;
        this.decodingFormat = decodingFormat;
        this.topic = topic;
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
        properties.computeIfAbsent("topic", (k) -> topic);
        this.properties = properties;
        this.startupOptions = startupOptions;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        DeserializationSchema<RowData> deserializationSchema =
                this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.outputDataType);

        FlinkPulsarSource<RowData> source = new FlinkPulsarSource<>(
                adminUrl,
                newClientConf(serviceUrl),
                new PulsarDeserializationSchemaWrapper<>(deserializationSchema),
                properties
        );
        // TODO 调整结构
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
                this.outputDataType,
                this.decodingFormat,
                this.topic,
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

    private static Properties removeConnectorPrefix(Properties in) {
        String connectorPrefix = CONNECTOR + ".";

        Properties out = new Properties();
        for (Map.Entry<Object, Object> kv : in.entrySet()) {
            String k = (String) kv.getKey();
            String v = (String) kv.getValue();
            if (k.startsWith(connectorPrefix)) {
                out.put(k.substring(connectorPrefix.length()), v);
            } else {
                out.put(k, v);
            }
        }
        return out;
    }

    private boolean checkForCustomFieldMapping(DescriptorProperties descriptorProperties, TableSchema schema) {
        final Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(
                descriptorProperties,
                Optional.of(
                        schema.toRowType())); // until FLINK-9870 is fixed we assume that the table schema is the output type
        return fieldMapping.size() != schema.getFieldNames().length ||
                !fieldMapping.entrySet().stream().allMatch(mapping -> mapping.getKey().equals(mapping.getValue()));
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        // TODO allow Pulsar timestamps to be used, watermarks can not be received from source
        new PulsarSchemaValidator(true, true, false).validate(descriptorProperties);
        new PulsarValidator().validate(descriptorProperties);
        return descriptorProperties;
    }

    private static class StartupOptions {
        private StartupMode startupMode;
        private Map<String, MessageId> specificOffsets;
        private String externalSubscriptionName;
    }

    private DeserializationSchema<RowData> getDeserializationSchema(Map<String, String> properties) {
        try {
            @SuppressWarnings("unchecked") final DeserializationSchemaFactory<RowData> formatFactory =
                    TableFactoryService.find(
                            DeserializationSchemaFactory.class,
                            properties,
                            this.getClass().getClassLoader());
            return formatFactory.createDeserializationSchema(properties);
        } catch (Exception e) {
            LOGGER.warn("get deserializer from properties failed. using pulsar inner schema instead.");
            return null;
        }
    }
}
