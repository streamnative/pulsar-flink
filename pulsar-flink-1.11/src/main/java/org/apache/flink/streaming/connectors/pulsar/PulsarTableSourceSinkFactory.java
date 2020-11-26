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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonSer;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarCatalogSupport;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.PulsarValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableSinkFactory;
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

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.USE_EXTEND_FIELD;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
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
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
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
    private boolean isInDDL;

    public PulsarTableSourceSinkFactory(Properties catalogProperties) {
        this.catalogProperties = catalogProperties;
        this.isInPulsarCatalog = catalogProperties.size() != 0;
        this.isInDDL = false;
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
        String formatType = null;
        if(isInPulsarCatalog){
            formatType = catalogProperties.getProperty(FORMAT_TYPE);
        }else{
            formatType = dp.getString(FORMAT_TYPE);
        }

        Optional<String> proctime = SchemaValidator.deriveProctimeAttribute(dp);
        List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = SchemaValidator.deriveRowtimeAttributes(dp);

        // see also FLINK-9870
       /* if (proctime.isPresent() || !rowtimeAttributeDescriptors.isEmpty() ||
                checkForCustomFieldMapping(dp, schema)) {
            throw new TableException("Time attributes and custom field mappings are not supported yet.");
        }*/

        Properties sinkProp;
        if (isInPulsarCatalog) {
            sinkProp = new Properties();
            sinkProp.putAll(catalogProperties);
        } else {
            sinkProp = getPulsarProperties(dp);
        }
        sinkProp.put(CONNECTOR_TOPIC, topic);
        sinkProp.put(FORMAT_TYPE, formatType);
        Properties result = removeConnectorPrefix(sinkProp);

        SerializationSchema<Row> serializationSchema = getSerializationSchema(properties);

        log.info("stream table sink use {} to serialize data", serializationSchema);

        return new PulsarTableSink(serviceUrl, adminUrl, schema, topic, result, serializationSchema);
    }

    @Override
    public TableSink<Row> createTableSink(TableSinkFactory.Context context) {
        Map<String, String> result = new HashMap<>(context.getTable().toProperties());
        if (!result.containsKey(CONNECTOR_TOPIC)){
            String topic = PulsarCatalogSupport.objectPath2TopicName(context.getObjectIdentifier().toObjectPath());
            result.put(CONNECTOR_TOPIC, topic);
        }
        return createStreamTableSink(result);
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
        String serviceUrl = descriptorProperties.getString(CONNECTOR_SERVICE_URL);
        String adminUrl = descriptorProperties.getString(CONNECTOR_ADMIN_URL);
        StartupOptions startupOptions = getStartupOptions(descriptorProperties);

        // if not in pulsar catalog, that is using DDL, get schema from DDL
        // else set schema to Optional.empty(), let it get from pulsar schema
        // current pulsar catalog just init pulsar admin client, and actual schema
        // should get from pulsar using pulsar admin client.
        Optional<TableSchema> schema = Optional.empty();
        if (isInPulsarCatalog || isInDDL) {
            schema = Optional.of(descriptorProperties.getTableSchema(SCHEMA));
        }

        Properties sourceProp;
        if (isInPulsarCatalog) {
            sourceProp = new Properties();
            sourceProp.putAll(catalogProperties);
        } else {
            sourceProp = getPulsarProperties(descriptorProperties);
        }

        boolean useExtendField = descriptorProperties.getOptionalBoolean(CONNECTOR + "." + USE_EXTEND_FIELD).orElse(false);
        sourceProp.put(CONNECTOR + "." + USE_EXTEND_FIELD, useExtendField ? "true" : "false");

        sourceProp.put(CONNECTOR_TOPIC, topic);
        Properties result = removeConnectorPrefix(sourceProp);

        DeserializationSchema<Row> deserializationSchema = null;
        Optional<Map<String, String>> fieldMapping = Optional.empty();
        //if (isInDDL) {
            deserializationSchema = getDeserializationSchema(properties);
            fieldMapping = Optional.ofNullable(deserializationSchema)
                    .map(DeserializationSchema::getProducedType)
                    .map(type -> SchemaValidator.deriveFieldMapping(descriptorProperties, Optional.of(type)));
        //}
        log.info("stream table source use {} to deserialize data", deserializationSchema);
        return new PulsarTableSource(
                schema,
                SchemaValidator.deriveProctimeAttribute(descriptorProperties),
                SchemaValidator.deriveRowtimeAttributes(descriptorProperties),
                fieldMapping,
                serviceUrl,
                adminUrl,
                result,
                deserializationSchema,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.externalSubscriptionName);
    }

    @Override
    public TableSource<Row> createTableSource(ObjectPath tablePath, CatalogTable table) {
        Map<String, String> props = new HashMap<>();
        props.putAll(table.toProperties());
        isInDDL = props.size() != 0;

        if (props.get(CONNECTOR_TOPIC) == null) {
            String topic = PulsarCatalogSupport.objectPath2TopicName(tablePath);
            props.put(CONNECTOR_TOPIC, topic);
        }

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

        // append mode
        context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);
        // pulsar
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PULSAR);
        // backwards compatibility
        context.put(CONNECTOR_PROPERTY_VERSION, "1");

        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // update mode
        properties.add(UPDATE_MODE);

        // Pulsar
        properties.add(CONNECTOR_VERSION);
        properties.add(CONNECTOR_TOPIC);
        properties.add(CONNECTOR_SERVICE_URL);
        properties.add(CONNECTOR_ADMIN_URL);

        properties.add(CONNECTOR_STARTUP_MODE);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_PARTITION);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_OFFSET);
        properties.add(CONNECTOR_PROPERTIES + ".*");
        properties.add(CONNECTOR_EXTERNAL_SUB_NAME);

        properties.add(CONNECTOR + "." + USE_EXTEND_FIELD);
        properties.add(CONNECTOR_PROPERTIES);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);

        properties.add(CONNECTOR_SINK_EXTRACTOR);
        properties.add(CONNECTOR_SINK_EXTRACTOR_CLASS);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + EXPR);

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

        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);

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

                        case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EXTERNAL_SUB:
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
        final String magicKey = CONNECTOR_PROPERTIES + ".0." + CONNECTOR_PROPERTIES_KEY;
        if (!descriptorProperties.containsKey(magicKey)) {
            descriptorProperties.asMap().keySet()
                    .stream()
                    .filter(key -> key.startsWith(CONNECTOR_PROPERTIES))
                    .forEach(key -> {
                        final String value = descriptorProperties.getString(key);
                        final String subKey = key.substring((CONNECTOR_PROPERTIES + '.').length());
                        pulsarProperties.put(subKey, value);
                    });
        } else {
            final List<Map<String, String>> propsList = descriptorProperties.getFixedIndexedProperties(
                    CONNECTOR_PROPERTIES,
                    Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE));
            propsList.forEach(kv -> pulsarProperties.put(
                    descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
                    descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_VALUE))
            ));
        }
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

    private DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
        try {
            @SuppressWarnings("unchecked")
            final DeserializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
                    DeserializationSchemaFactory.class,
                    properties,
                    this.getClass().getClassLoader());
            return formatFactory.createDeserializationSchema(properties);
        } catch (Exception e) {
            log.warn("get deserializer from properties failed. using pulsar inner schema instead.");
            return null;
        }
    }

    private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
        try {
            @SuppressWarnings("unchecked")
            final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
                    SerializationSchemaFactory.class,
                    properties,
                    this.getClass().getClassLoader());
            return formatFactory.createSerializationSchema(properties);
        } catch (Exception e) {
            log.warn("get deserializer from properties failed. using json schema instead.");
            return JsonSer.of(Row.class);
        }
    }
}
