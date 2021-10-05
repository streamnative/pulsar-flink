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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarCatalogSupport;
import org.apache.flink.streaming.connectors.pulsar.util.KeyHashMessageRouterImpl;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
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

import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.CONNECTOR;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PROPERTIES;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_SUB_NAME;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_SUB_START_OFFSET;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SERVICE_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SINK_MESSAGE_ROUTER;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SINK_SEMANTIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC_PATTERN;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.getMessageRouter;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.getPulsarProperties;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.validateSinkMessageRouter;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.validateTableSourceOptions;
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

    public static final String UPSERT_CONNECTOR = "upsert-pulsar";

    private final boolean inCatalog;

    public PulsarDynamicTableFactory() {
        this.inCatalog = false;
    }

    public PulsarDynamicTableFactory(boolean inCatalog) {
        this.inCatalog = inCatalog;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        if (tableOptions.get(CONNECTOR).equals(UPSERT_CONNECTOR)) {
            return createDynamicTableSinkUpsert(context);
        }

        if (inCatalog) {
            final ObjectIdentifier table = context.getObjectIdentifier();
            final String topic = TopicName.get(table.getDatabaseName() + "/" + table.getObjectName()).toString();
            ((Configuration) tableOptions).set(TOPIC, Collections.singletonList(topic));
        }
        List<String> topics = tableOptions.get(TOPIC);
        String adminUrl = tableOptions.get(ADMIN_URL);
        String serverUrl = tableOptions.get(SERVICE_URL);
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX, "type", "table-default-partitions", "default-database");
        // Validate the option values.
        PulsarTableOptions.validateTableSinkOptions(tableOptions);

        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());
        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueEncodingFormat);

        final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return createPulsarTableSink(tableOptions, topics, adminUrl, serverUrl, keyEncodingFormat, valueEncodingFormat,
                properties, physicalDataType, keyProjection, valueProjection, keyPrefix, context);
    }

    private PulsarDynamicTableSink createPulsarTableSink(ReadableConfig tableOptions, List<String> topics,
                                                         String adminUrl, String serverUrl,
                                                         Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat,
                                                         EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
                                                         Properties properties, DataType physicalDataType,
                                                         int[] keyProjection, int[] valueProjection,
                                                         String keyPrefix, Context context) {

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
                PulsarTableOptions.getSinkSemantic(tableOptions),
                formatType,
                false,
                parallelism,
                getMessageRouter(tableOptions, context.getClassLoader()).orElse(null));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        if (tableOptions.get(CONNECTOR).equals(UPSERT_CONNECTOR)) {
            return createDynamicTableSourceUpsert(context);
        }

        List<String> topics = new ArrayList<>();
        String topicPattern = null;
        if (PulsarCatalogSupport.isNativeFlinkDatabase(context.getObjectIdentifier().getDatabaseName())) {
            topics = tableOptions.get(TOPIC);
            if (topics != null && !topics.isEmpty()) {
                ((Configuration) tableOptions).set(TOPIC, Collections.singletonList(topics.get(0)));
            }

            topicPattern = tableOptions.get(TOPIC_PATTERN);
            if (topicPattern != null) {
                ((Configuration) tableOptions).set(TOPIC_PATTERN, topicPattern);
            }
        } else {
            final ObjectIdentifier table = context.getObjectIdentifier();
            final String topic = TopicName.get(table.getDatabaseName() + "/" + table.getObjectName()).toString();
            ((Configuration) tableOptions).set(TOPIC, Collections.singletonList(topic));
            topics.add(topic);
        }

        String adminUrl = tableOptions.get(ADMIN_URL);
        String serviceUrl = tableOptions.get(SERVICE_URL);

        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);
        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX, "type", "table-default-partitions", "default-database");
        // Validate the option values.
        validateTableSourceOptions(tableOptions);
        validateSinkMessageRouter(tableOptions);
        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueDecodingFormat);

        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());

        final PulsarTableOptions.StartupOptions startupOptions = PulsarTableOptions
                .getStartupOptions(tableOptions);

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
        options.add(SCAN_STARTUP_SUB_START_OFFSET);

        options.add(PARTITION_DISCOVERY_INTERVAL_MILLIS);
        options.add(SINK_SEMANTIC);
        options.add(SINK_MESSAGE_ROUTER);
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
            PulsarTableOptions.StartupOptions startupOptions) {
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

    public DynamicTableSource createDynamicTableSourceUpsert(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();
        DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat = helper.discoverDecodingFormat(
            DeserializationFormatFactory.class,
            KEY_FORMAT);
        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat = helper.discoverDecodingFormat(
            DeserializationFormatFactory.class,
            VALUE_FORMAT);

        String adminUrl = tableOptions.get(ADMIN_URL);
        String serverUrl = tableOptions.get(SERVICE_URL);
        List<String> topics = tableOptions.get(TOPIC);
        String topicPattern = tableOptions.get(TOPIC_PATTERN);

        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);
        TableSchema schema = context.getCatalogTable().getSchema();
        validateTableOptionsUpsert(tableOptions, keyDecodingFormat, valueDecodingFormat, schema);

        Tuple2<int[], int[]> keyValueProjections = createKeyValueProjectionsUpsert(context.getCatalogTable());
        String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());

        // always use earliest to keep data integrity
        PulsarTableOptions.StartupOptions startupOptions = new PulsarTableOptions.StartupOptions();
        startupOptions.startupMode = StartupMode.EARLIEST;
        startupOptions.specificOffsets = Collections.EMPTY_MAP;

        return new PulsarDynamicTableSource(
            schema.toPhysicalRowDataType(),
            keyDecodingFormat,
            new PulsarDynamicTableFactory.DecodingFormatWrapper(valueDecodingFormat),
            keyValueProjections.f0,
            keyValueProjections.f1,
            keyPrefix,
            topics,
            topicPattern,
            serverUrl,
            adminUrl,
            properties,
            startupOptions,
            true);
    }

    private static void validateTableOptionsUpsert(
        ReadableConfig tableOptions,
        Format keyFormat,
        Format valueFormat,
        TableSchema schema) {
        validateTopicUpsert(tableOptions);
        validateFormatUpsert(keyFormat, valueFormat, tableOptions);
        validatePKConstraintsUpsert(schema);
    }

    private static void validateTopicUpsert(ReadableConfig tableOptions) {
        List<String> topic = tableOptions.get(TOPIC);
        if (topic.size() > 1) {
            throw new ValidationException("The 'upsert-pulsar' connector doesn't support topic list now. " +
                "Please use single topic as the value of the parameter 'topic'.");
        }
    }

    private static void validateFormatUpsert(Format keyFormat, Format valueFormat, ReadableConfig tableOptions) {
        if (!keyFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            String identifier = tableOptions.get(KEY_FORMAT);
            throw new ValidationException(String.format(
                "'upsert-pulsar' connector doesn't support '%s' as key format, " +
                    "because '%s' is not in insert-only mode.",
                identifier,
                identifier));
        }
        if (!valueFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            String identifier = tableOptions.get(VALUE_FORMAT);
            throw new ValidationException(String.format(
                "'upsert-Pulsar' connector doesn't support '%s' as value format, " +
                    "because '%s' is not in insert-only mode.",
                identifier,
                identifier));
        }

    }

    private static void validatePKConstraintsUpsert(TableSchema schema) {
        if (!schema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                "'upsert-pulsar' tables require to define a PRIMARY KEY constraint. " +
                    "The PRIMARY KEY specifies which columns should be read from or write to the Pulsar message key. " +
                    "The PRIMARY KEY also defines records in the 'upsert-pulsar' table should update or delete on which keys.");
        }
    }

    private Tuple2<int[], int[]> createKeyValueProjectionsUpsert(CatalogTable catalogTable) {
        TableSchema schema = catalogTable.getSchema();
        // primary key should validated earlier
        List<String> keyFields = schema.getPrimaryKey().get().getColumns();
        DataType physicalDataType = schema.toPhysicalRowDataType();

        Configuration tableOptions = Configuration.fromMap(catalogTable.getOptions());
        // upsert-pulsar will set key.fields to primary key fields by default
        tableOptions.set(KEY_FIELDS, keyFields);

        int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
        int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        return Tuple2.of(keyProjection, valueProjection);
    }

    private DynamicTableSink createDynamicTableSinkUpsert(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat = helper.discoverEncodingFormat(
            SerializationFormatFactory.class,
            KEY_FORMAT);
        EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat = helper.discoverEncodingFormat(
            SerializationFormatFactory.class,
            VALUE_FORMAT);

        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);
        TableSchema schema = context.getCatalogTable().getSchema();
        validateTableOptionsUpsert(tableOptions, keyEncodingFormat, valueEncodingFormat, schema);

        Tuple2<int[], int[]> keyValueProjections = createKeyValueProjectionsUpsert(context.getCatalogTable());
        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());
        Integer parallelism = tableOptions.get(SINK_PARALLELISM);

        String adminUrl = tableOptions.get(ADMIN_URL);
        String serverUrl = tableOptions.get(SERVICE_URL);
        String formatType = tableOptions.getOptional(FORMAT).orElseGet(() -> tableOptions.get(VALUE_FORMAT));

        return new PulsarDynamicTableSink(
            serverUrl,
            adminUrl,
            tableOptions.get(TOPIC).get(0),
            schema.toPhysicalRowDataType(),
            properties,
            keyEncodingFormat,
            new PulsarDynamicTableFactory.EncodingFormatWrapper(valueEncodingFormat),
            keyValueProjections.f0,
            keyValueProjections.f1,
            keyPrefix,
            PulsarSinkSemantic.AT_LEAST_ONCE,
            formatType,
            true,
            parallelism,
            KeyHashMessageRouterImpl.INSTANCE);
    }

    // --------------------------------------------------------------------------------------------
    // Format wrapper
    // --------------------------------------------------------------------------------------------

    /**
     * It is used to wrap the decoding format and expose the desired changelog mode. It's only works
     * for insert-only format.
     */
    protected static class DecodingFormatWrapper implements DecodingFormat<DeserializationSchema<RowData>> {
        private final DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat;

        private static final ChangelogMode SOURCE_CHANGELOG_MODE = ChangelogMode.newBuilder()
            .addContainedKind(RowKind.UPDATE_AFTER)
            .addContainedKind(RowKind.DELETE)
            .build();

        public DecodingFormatWrapper(DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat) {
            this.innerDecodingFormat = innerDecodingFormat;
        }

        @Override
        public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
            return innerDecodingFormat.createRuntimeDecoder(context, producedDataType);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return SOURCE_CHANGELOG_MODE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            PulsarDynamicTableFactory.DecodingFormatWrapper that = (PulsarDynamicTableFactory.DecodingFormatWrapper) obj;
            return Objects.equals(innerDecodingFormat, that.innerDecodingFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerDecodingFormat);
        }
    }

    /**
     * It is used to wrap the encoding format and expose the desired changelog mode. It's only works
     * for insert-only format.
     */
    protected static class EncodingFormatWrapper implements EncodingFormat<SerializationSchema<RowData>> {
        private final EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat;

        public static final ChangelogMode SINK_CHANGELOG_MODE = ChangelogMode.newBuilder()
            .addContainedKind(RowKind.INSERT)
            .addContainedKind(RowKind.UPDATE_AFTER)
            .addContainedKind(RowKind.DELETE)
            .build();

        public EncodingFormatWrapper(EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat) {
            this.innerEncodingFormat = innerEncodingFormat;
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType consumedDataType) {
            return innerEncodingFormat.createRuntimeEncoder(context, consumedDataType);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return SINK_CHANGELOG_MODE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            PulsarDynamicTableFactory.EncodingFormatWrapper that = (PulsarDynamicTableFactory.EncodingFormatWrapper) obj;
            return Objects.equals(innerEncodingFormat, that.innerEncodingFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerEncodingFormat);
        }
    }
}
