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

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.ADMIN_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.PROPERTIES;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.PULSAR_READER_READER_NAME;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.PULSAR_READER_RECEIVER_QUEUE_SIZE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.PULSAR_READER_SUBSCRIPTION_ROLE_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.SCAN_STARTUP_SUB_NAME;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.SERVICE_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.SINK_SEMANTIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.TOPIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.TOPIC_PATTERN;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.getPulsarProperties;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarOptions.validateTableSourceOptions;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
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

        List<String> topics = tableOptions.get(TOPIC);
        String adminUrl = tableOptions.get(ADMIN_URL);
        String serverUrl = tableOptions.get(SERVICE_URL);
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);
        // Validate the option values.
        PulsarOptions.validateTableSinkOptions(tableOptions);

        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());
        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueEncodingFormat);

        final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return createPulsarTableSink(tableOptions, topics, adminUrl, serverUrl, keyEncodingFormat, valueEncodingFormat,
                properties, physicalDataType, keyProjection, valueProjection, keyPrefix);
    }

    private PulsarDynamicTableSink createPulsarTableSink(ReadableConfig tableOptions, List<String> topics,
                                                         String adminUrl, String serverUrl,
                                                         Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat,
                                                         EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
                                                         Properties properties, DataType physicalDataType,
                                                         int[] keyProjection, int[] valueProjection, String keyPrefix) {

        final String formatType = tableOptions.getOptional(FORMAT).orElseGet(() -> tableOptions.get(VALUE_FORMAT));
        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
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
                PulsarOptions.getSinkSemantic(tableOptions),
                formatType,
                false,
                parallelism,
                null);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        List<String> topics = tableOptions.get(TOPIC);
        String topicPattern = tableOptions.get(TOPIC_PATTERN);
        String adminUrl = tableOptions.get(ADMIN_URL);
        String serviceUrl = tableOptions.get(SERVICE_URL);

        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);
        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);
        // Validate the option values.
        validateTableSourceOptions(tableOptions);

        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueDecodingFormat);

        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());

        final PulsarOptions.StartupOptions startupOptions = PulsarOptions.getStartupOptions(tableOptions, topics);

        final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return createPulsarTableSource(
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                serviceUrl,
                adminUrl,
                properties,
                startupOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SERVICE_URL);
        options.add(ADMIN_URL);
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
        options.add(TOPIC);
        options.add(TOPIC_PATTERN);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_SUB_NAME);

        options.add(PULSAR_READER_READER_NAME);
        options.add(PULSAR_READER_SUBSCRIPTION_ROLE_PREFIX);
        options.add(PULSAR_READER_RECEIVER_QUEUE_SIZE);
        options.add(PARTITION_DISCOVERY_INTERVAL_MILLIS);
        options.add(SINK_SEMANTIC);
        options.add(SINK_PARALLELISM);
        options.add(PROPERTIES);
        return options;
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
        if (catalogTable.getSchema().getPrimaryKey().isPresent() &&
                format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration options = Configuration.fromMap(catalogTable.getOptions());
            String formatName = options.getOptional(FactoryUtil.FORMAT).orElse(options.get(VALUE_FORMAT));
            throw new ValidationException(String.format(
                    "The Pulsar table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint" +
                            " on the table, because it can't guarantee the semantic of primary key.",
                    tableName.asSummaryString(),
                    formatName
            ));
        }
    }

    // --------------------------------------------------------------------------------------------
    protected PulsarDynamicTableSource createPulsarTableSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> topics,
            @Nullable String topicPattern,
            String serviceUrl,
            String adminUrl,
            Properties properties,
            PulsarOptions.StartupOptions startupOptions) {
        return new PulsarDynamicTableSource(
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                serviceUrl,
                adminUrl,
                properties,
                startupOptions,
                false);
    }

//    protected PulsarDynamicSink createPulsarTableSink(
//            DataType physicalDataType,
//            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
//            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
//            int[] keyProjection,
//            int[] valueProjection,
//            @Nullable String keyPrefix,
//            String topic,
//            Properties properties,
//            FlinkPulsarPartitioner<RowData> partitioner,
//            PulsarSinkSemantic semantic,
//            Integer parallelism) {
//        return new PulsarDynamicSink(
//                physicalDataType,
//                keyEncodingFormat,
//                valueEncodingFormat,
//                keyProjection,
//                valueProjection,
//                keyPrefix,
//                topic,
//                properties,
//                partitioner,
//                semantic,
//                false,
//                parallelism);
//    }
}
