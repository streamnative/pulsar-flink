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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.SINK_SEMANTIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.VALUE_FORMAT;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/**
 * Factory for creating configured instances of
 * {@link PulsarDynamicTableFactory}.
 */
public class PulsarDynamicTableFactory implements
        DynamicTableSourceFactory,
        DynamicTableSinkFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        List<String> topics = tableOptions.get(PulsarOptions.TOPIC);
        String adminUrl = tableOptions.get(PulsarOptions.ADMIN_URL);
        String serverUrl = tableOptions.get(PulsarOptions.SERVICE_URL);
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        // Validate the option data type.
        helper.validateExcept(PulsarOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        PulsarOptions.validateTableSinkOptions(tableOptions);

        Properties properties = removeConnectorPrefix(context.getCatalogTable().toProperties());

        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueEncodingFormat);

        final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] keyProjection = PulsarOptions.createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = PulsarOptions.createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return new PulsarDynamicTableSink(
                serverUrl,
                adminUrl,
                topics.get(0),
                physicalDataType,
                properties,
                keyEncodingFormat.orElse(null),
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                PulsarOptions.getSinkSemantic(tableOptions)
        );
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        List<String> topics = tableOptions.get(PulsarOptions.TOPIC);
        String topicPattern = tableOptions.get(PulsarOptions.TOPIC_PATTERN);
        String adminUrl = tableOptions.get(PulsarOptions.ADMIN_URL);
        String serviceUrl = tableOptions.get(PulsarOptions.SERVICE_URL);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PulsarOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        PulsarOptions.validateTableSourceOptions(tableOptions);
        Properties properties = removeConnectorPrefix(context.getCatalogTable().toProperties());

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        final PulsarOptions.StartupOptions startupOptions = PulsarOptions.getStartupOptions(tableOptions, topics);
        return new PulsarDynamicTableSource(
                producedDataType,
                decodingFormat,
                topics,
                topicPattern,
                serviceUrl,
                adminUrl,
                properties,
                startupOptions
        );
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PulsarOptions.SERVICE_URL);
        options.add(PulsarOptions.ADMIN_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FORMAT);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(PulsarOptions.TOPIC);
        options.add(PulsarOptions.TOPIC_PATTERN);
        options.add(PulsarOptions.SCAN_STARTUP_MODE);
        options.add(PulsarOptions.SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(PulsarOptions.SCAN_STARTUP_SUB_NAME);

        options.add(PulsarOptions.PULSAR_READER_READER_NAME);
        options.add(PulsarOptions.PULSAR_READER_SUBSCRIPTION_ROLE_PREFIX);
        options.add(PulsarOptions.PULSAR_READER_RECEIVER_QUEUE_SIZE);
        options.add(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MILLIS);
        options.add(SINK_SEMANTIC);
        options.add(SINK_PARALLELISM);
        return options;
    }

    private static Properties removeConnectorPrefix(Map<String, String> in) {
        String connectorPrefix = CONNECTOR + ".";
        Properties out = new Properties();
        for (Map.Entry<String, String> kv : in.entrySet()) {
            String k = kv.getKey();
            String v = kv.getValue();
            if (k.startsWith(connectorPrefix)) {
                out.put(k.substring(connectorPrefix.length()), v);
            } else {
                out.put(k, v);
            }
        }
        return out;
    }

    // --------------------------------------------------------------------------------------------

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class,
                        KEY_FORMAT);
        keyDecodingFormat.ifPresent(format -> {
            if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                throw new ValidationException(
                        String.format(
                                "A key format should only deal with INSERT-only records. "
                                        + "But %s has a changelog mode of %s.",
                                helper.getOptions().get(KEY_FORMAT),
                                format.getChangelogMode()));
            }
        });
        return keyDecodingFormat;
    }

    private static Optional<EncodingFormat<SerializationSchema<RowData>>> getKeyEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                helper.discoverOptionalEncodingFormat(
                        SerializationFormatFactory.class,
                        KEY_FORMAT);
        keyEncodingFormat.ifPresent(format -> {
            if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                throw new ValidationException(
                        String.format(
                                "A key format should only deal with INSERT-only records. "
                                        + "But %s has a changelog mode of %s.",
                                helper.getOptions().get(KEY_FORMAT),
                                format.getChangelogMode()));
            }
        });
        return keyEncodingFormat;
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(() -> helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(() -> helper.discoverEncodingFormat(SerializationFormatFactory.class, VALUE_FORMAT));
    }

    private static void validatePKConstraints(ObjectIdentifier tableName, CatalogTable catalogTable, Format format) {
        if (catalogTable.getSchema().getPrimaryKey().isPresent() && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration options = Configuration.fromMap(catalogTable.getOptions());
            String formatName = options.getOptional(FactoryUtil.FORMAT).orElse(options.get(VALUE_FORMAT));
            throw new ValidationException(String.format(
                    "The Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint" +
                            " on the table, because it can't guarantee the semantic of primary key.",
                    tableName.asSummaryString(),
                    formatName
            ));
        }
    }

    // --------------------------------------------------------------------------------------------

}
