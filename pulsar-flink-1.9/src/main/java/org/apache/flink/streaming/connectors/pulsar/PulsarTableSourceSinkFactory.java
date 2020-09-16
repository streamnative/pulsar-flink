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

import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarCatalogSupport;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.PulsarValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_ADMIN_URL;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_NAME;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_PROPERTIES_VALUE;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_SERVICE_URL;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_SINK_EXTRACTOR;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_SINK_EXTRACTOR_CLASS;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_TOPIC;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_TYPE_VALUE_PULSAR;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * Pulsar Table source sink factory.
 */
@Slf4j
public class PulsarTableSourceSinkFactory
        implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Row> {

    private Properties catalogProperties;

    private boolean isInPulsarCatalog;

    public PulsarTableSourceSinkFactory(Properties catalogProperties) {
        this.catalogProperties = catalogProperties;
        this.isInPulsarCatalog = catalogProperties.size() != 0;
    }

    public PulsarTableSourceSinkFactory() {
        this(new Properties());
    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        DescriptorProperties dp = getValidatedProperties(properties);
        TableSchema schema = dp.getTableSchema(SCHEMA);

        final String topic = dp.getString(CONNECTOR_TOPIC);
        String serviceUrl = dp.getString(CONNECTOR_SERVICE_URL);
        String adminUrl = dp.getString(CONNECTOR_ADMIN_URL);

        Optional<String> proctime = SchemaValidator.deriveProctimeAttribute(dp);
        List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = SchemaValidator.deriveRowtimeAttributes(dp);

        // see also FLINK-9870
        if (proctime.isPresent() || !rowtimeAttributeDescriptors.isEmpty() ||
                checkForCustomFieldMapping(dp, schema)) {
            throw new TableException("Time attributes and custom field mappings are not supported yet.");
        }

        Properties sinkProp;
        if (isInPulsarCatalog) {
            sinkProp = new Properties();
            sinkProp.putAll(catalogProperties);
        } else {
            sinkProp = getPulsarProperties(dp);
        }
        sinkProp.put(CONNECTOR_TOPIC, topic);

        Properties result = removeConnectorPrefix(sinkProp);

        return new PulsarTableSink(serviceUrl, adminUrl, schema, Optional.of(topic), result);
    }

    @Override
    public TableSink<Row> createTableSink(ObjectPath tablePath, CatalogTable table) {
        String topic = PulsarCatalogSupport.objectPath2TopicName(tablePath);

        Map<String, String> props = new HashMap<String, String>();
        props.putAll(table.toProperties());
        props.put(CONNECTOR_TOPIC, topic);

        return createStreamTableSink(props);
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
        String serviceUrl = descriptorProperties.getString(CONNECTOR_SERVICE_URL);
        String adminUrl = descriptorProperties.getString(CONNECTOR_ADMIN_URL);
        StartupOptions startupOptions = getStartupOptions(descriptorProperties);

        Optional<TableSchema> schema;
        if (isInPulsarCatalog) {
            schema = Optional.of(descriptorProperties.getTableSchema(SCHEMA));
        } else {
            schema = Optional.empty();
        }

        Properties sourceProp;
        if (isInPulsarCatalog) {
            sourceProp = new Properties();
            sourceProp.putAll(catalogProperties);
        } else {
            sourceProp = getPulsarProperties(descriptorProperties);
        }
        sourceProp.put(CONNECTOR_TOPIC, topic);

        Properties result = removeConnectorPrefix(sourceProp);

        return new PulsarTableSource(
                schema,
                SchemaValidator.deriveProctimeAttribute(descriptorProperties),
                SchemaValidator.deriveRowtimeAttributes(descriptorProperties),
                serviceUrl,
                adminUrl,
                result,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.externalSubscriptionName);
    }

    @Override
    public TableSource<Row> createTableSource(ObjectPath tablePath, CatalogTable table) {
        String topic = PulsarCatalogSupport.objectPath2TopicName(tablePath);

        Map<String, String> props = new HashMap<>();
        props.putAll(table.toProperties());
        props.put(CONNECTOR_TOPIC, topic);

        return createStreamTableSource(props);
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

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();

        context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND); // append mode
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PULSAR); // pulsar
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility

        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // Pulsar
        properties.add(CONNECTOR_TOPIC);
        properties.add(CONNECTOR_SERVICE_URL);
        properties.add(CONNECTOR_ADMIN_URL);

        properties.add(CONNECTOR_STARTUP_MODE);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_PARTITION);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_OFFSET);

        properties.add(CONNECTOR_PROPERTIES);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);

        properties.add(CONNECTOR_SINK_EXTRACTOR);
        properties.add(CONNECTOR_SINK_EXTRACTOR_CLASS);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // format wildcard
        properties.add(FORMAT + ".*");

        return properties;
    }

    private StartupOptions getStartupOptions(DescriptorProperties descriptorProperties) {
        final Map<String, MessageId> specificOffsets = new HashMap<>();
        final List<String> subName = new ArrayList<>(1);
        final StartupMode startupMode = descriptorProperties
                .getOptionalString(CONNECTOR_STARTUP_MODE)
                .map(modeString -> {
                    switch (modeString) {
                        case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
                            return StartupMode.EARLIEST;

                        case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
                            return StartupMode.LATEST;

                        case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
                            final List<Map<String, String>> offsetList = descriptorProperties.getFixedIndexedProperties(
                                    CONNECTOR_SPECIFIC_OFFSETS,
                                    Arrays.asList(CONNECTOR_SPECIFIC_OFFSETS_PARTITION, CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
                            offsetList.forEach(kv -> {
                                final String partition = descriptorProperties.getString(kv.get(CONNECTOR_SPECIFIC_OFFSETS_PARTITION));
                                final String offset = descriptorProperties.getString(kv.get(CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
                                try {
                                    specificOffsets.put(partition, MessageId.fromByteArray(offset.getBytes()));
                                } catch (IOException e) {
                                    log.error("Failed to decode message id from properties {}", ExceptionUtils.stringifyException(e));
                                    throw new RuntimeException(e);
                                }
                            });
                            return StartupMode.SPECIFIC_OFFSETS;

                        case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EXETERNAL_SUB:
                            subName.add(descriptorProperties.getString(CONNECTOR_EXTERNAL_SUB_NAME));
                            return StartupMode.EXTERNAL_SUBSCRIPTION;

                        default:
                            throw new TableException("Unsupported startup mode. Validator should have checked that.");
                    }
                }).orElse(StartupMode.LATEST);
        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (subName.size() != 0) {
            options.externalSubscriptionName = subName.get(0);
        }
        return options;
    }

    private Properties getPulsarProperties(DescriptorProperties descriptorProperties) {
        final Properties pulsarProperties = new Properties();
        final List<Map<String, String>> propsList = descriptorProperties.getFixedIndexedProperties(
                CONNECTOR_PROPERTIES,
                Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE));
        propsList.forEach(kv -> pulsarProperties.put(
                descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
                descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_VALUE))
        ));
        return pulsarProperties;
    }

    private boolean checkForCustomFieldMapping(DescriptorProperties descriptorProperties, TableSchema schema) {
        final Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(
                descriptorProperties,
                Optional.of(schema.toRowType())); // until FLINK-9870 is fixed we assume that the table schema is the output type
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
}
