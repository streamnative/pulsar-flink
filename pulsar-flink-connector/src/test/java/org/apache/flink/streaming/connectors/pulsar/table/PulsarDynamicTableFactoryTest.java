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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.util.KeyHashMessageRouterImpl;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.factories.TestFormatFactory.EncodingFormatMock;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.descriptors.DescriptorProperties.METADATA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract test base for {@link PulsarDynamicTableFactory}.
 */
public class PulsarDynamicTableFactoryTest extends TestLogger {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String ADMIN_URL = "http://127.0.0.1:8080";
    private static final String TOPIC = "myTopic";
    private static final String TOPICS = "myTopic-1;myTopic-2;myTopic-3";
    private static final String TOPIC_REGEX = "myTopic-\\d+";
    private static final List<String> TOPIC_LIST = Arrays.asList("myTopic-1", "myTopic-2", "myTopic-3");

    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String EVENT_TIME = "eventTime";
    private static final String METADATA_TOPIC = "topic";
    private static final String WATERMARK_EXPRESSION = EVENT_TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);
    private static final String DISCOVERY_INTERVAL = "1000";

    private static final Properties PULSAR_SOURCE_PROPERTIES = new Properties();
    private static final Properties PULSAR_SINK_PROPERTIES = new Properties();

    static {

        PULSAR_SOURCE_PROPERTIES.setProperty("partition.discovery.interval-millis", "1000");
        PULSAR_SOURCE_PROPERTIES.setProperty("pulsar.reader.readername", "testReaderName");

        PULSAR_SINK_PROPERTIES.setProperty("pulsar.reader.readername", "testReaderName");

    }

    private static final String PROPS_SCAN_OFFSETS =
            MessageId.earliest.toString();

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(NAME, DataTypes.STRING().notNull()),
                            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                            Column.physical(EVENT_TIME, DataTypes.TIMESTAMP(3)),
                            Column.computed(
                                    COMPUTED_COLUMN_NAME,
                                    ResolvedExpressionMock.of(
                                            COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION))),
                    Collections.singletonList(
                            WatermarkSpec.of(
                                    EVENT_TIME,
                                    ResolvedExpressionMock.of(
                                            WATERMARK_DATATYPE, WATERMARK_EXPRESSION))),
                    null);

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(NAME, DataTypes.STRING()),
                            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                            Column.metadata(EVENT_TIME, DataTypes.TIMESTAMP(3), "eventTime", false),
                            Column.metadata(
                                    METADATA, DataTypes.STRING(), "value.metadata_2", false)),
                    Collections.emptyList(),
                    null);

    private static final DataType SCHEMA_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

    @Test
    public void testTableSource() {
        final DynamicTableSource actualSource = createTableSource(SCHEMA, getBasicSourceOptions());
        final PulsarDynamicTableSource actualPulsarSource = (PulsarDynamicTableSource) actualSource;

        final Map<String, MessageId> specificOffsets = new HashMap<>();
        specificOffsets.put("-1", MessageId.earliest);
//        specificOffsets.put(TOPIC, MessageId.earliest);

        final PulsarTableOptions.StartupOptions startupOptions = new PulsarTableOptions.StartupOptions();
        startupOptions.startupMode = StartupMode.SPECIFIC_OFFSETS;
        startupOptions.specificOffsets = specificOffsets;

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);

        // Test scan source equals
        final PulsarDynamicTableSource expectedPulsarSource = createExpectedScanSource(
                SCHEMA_DATA_TYPE,
                null,
                valueDecodingFormat,
                new int[0],
                new int[]{0, 1, 2},
                null,
                SERVICE_URL,
                ADMIN_URL,
                Collections.singletonList(TOPIC),
                null,
                PULSAR_SOURCE_PROPERTIES,
                startupOptions);
        assertEquals(actualPulsarSource, expectedPulsarSource);

        // Test Pulsar consumer
        ScanTableSource.ScanRuntimeProvider provider =
                actualPulsarSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider, instanceOf(SourceFunctionProvider.class));
        final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
        final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
        assertThat(sourceFunction, instanceOf(FlinkPulsarSource.class));

        // Test commitOnCheckpoints flag should be true when set consumer group
//		assertTrue(((FlinkPulsarSource<?>) sourceFunction).getEnableCommitOnCheckpoints());
    }

    @Test
    public void testTableSourceCommitOnCheckpointsDisabled() {
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> options.remove("properties.group.id"));
        final DynamicTableSource tableSource = createTableSource(SCHEMA, modifiedOptions);

        // Test commitOnCheckpoints flag should be false when do not set consumer group.
        assertThat(tableSource, instanceOf(PulsarDynamicTableSource.class));
        ScanTableSource.ScanRuntimeProvider providerWithoutGroupId = ((PulsarDynamicTableSource) tableSource)
                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(providerWithoutGroupId, instanceOf(SourceFunctionProvider.class));
        final SourceFunctionProvider functionProviderWithoutGroupId = (SourceFunctionProvider) providerWithoutGroupId;
        final SourceFunction<RowData> function = functionProviderWithoutGroupId.createSourceFunction();
//		assertFalse(((FlinkPulsarSource<?>) function).getEnableCommitOnCheckpoints());
    }

    @Test
    public void testTableSourceWithPattern() {
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.remove("topic");
                    options.put("topic-pattern", TOPIC_REGEX);
                    options.put("scan.startup.mode", "earliest");
                    options.remove("scan.startup.specific-offsets");
                });
        final DynamicTableSource actualSource = createTableSource(SCHEMA, modifiedOptions);

        final PulsarTableOptions.StartupOptions startupOptions = new PulsarTableOptions.StartupOptions();
        startupOptions.startupMode = StartupMode.EARLIEST;
        startupOptions.specificOffsets = new HashMap<>();

        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);

        // Test scan source equals
        final PulsarDynamicTableSource expectedPulsarSource = createExpectedScanSource(
                SCHEMA_DATA_TYPE,
                null,
                valueDecodingFormat,
                new int[0],
                new int[]{0, 1, 2},
                null,
                SERVICE_URL,
                ADMIN_URL,
                null,
                TOPIC_REGEX,
                PULSAR_SOURCE_PROPERTIES,
                startupOptions
        );
        final PulsarDynamicTableSource actualPulsarSource = (PulsarDynamicTableSource) actualSource;
        assertEquals(actualPulsarSource, expectedPulsarSource);

        // Test Pulsar consumer
        ScanTableSource.ScanRuntimeProvider provider =
                actualPulsarSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider, instanceOf(SourceFunctionProvider.class));
        final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
        final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
        assertThat(sourceFunction, instanceOf(FlinkPulsarSource.class));
    }

    @Test
    public void testTableSourceWithKeyValueAndMetadata() {
        final Map<String, String> options = getKeyValueOptions();
        options.put("value.test-format.readable-metadata", "eventTime:INT, metadata_2:STRING");

        final DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, options);
        final PulsarDynamicTableSource actualPulsarSource = (PulsarDynamicTableSource) actualSource;
        // initialize stateful testing formats
        actualPulsarSource.applyReadableMetadata(Arrays.asList("eventTime", "value.metadata_2"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualPulsarSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        final DecodingFormatMock expectedKeyFormat = new DecodingFormatMock(
                "#",
                false,
                ChangelogMode.insertOnly(),
                Collections.emptyMap());
        expectedKeyFormat.producedDataType = DataTypes.ROW(
                DataTypes.FIELD(NAME, DataTypes.STRING()))
                .notNull();

        final Map<String, DataType> expectedReadableMetadata = new LinkedHashMap<>();
        expectedReadableMetadata.put("eventTime", DataTypes.INT());
        expectedReadableMetadata.put("metadata_2", DataTypes.STRING());

        final DecodingFormatMock expectedValueFormat = new DecodingFormatMock(
                "|",
                false,
                ChangelogMode.insertOnly(),
                expectedReadableMetadata);
        expectedValueFormat.producedDataType = DataTypes.ROW(
                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                DataTypes.FIELD("metadata_2", DataTypes.STRING()))
                .notNull();
        expectedValueFormat.metadataKeys = Collections.singletonList("metadata_2");

        final PulsarTableOptions.StartupOptions startupOptions = new PulsarTableOptions.StartupOptions();
        startupOptions.startupMode = StartupMode.LATEST;
        startupOptions.specificOffsets = new HashMap<>();

        final PulsarDynamicTableSource expectedPulsarSource = createExpectedScanSource(
                SCHEMA_WITH_METADATA.toPhysicalRowDataType(),
                expectedKeyFormat,
                expectedValueFormat,
                new int[]{0},
                new int[]{1},
                null,
                SERVICE_URL,
                ADMIN_URL,
                Collections.singletonList(TOPIC),
                null,
                PULSAR_SOURCE_PROPERTIES,
                startupOptions
        );
        expectedPulsarSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedPulsarSource.metadataKeys = Collections.singletonList("eventTime");

        assertEquals(actualSource, expectedPulsarSource);
    }

    @Test
    public void testTableSink() {
        final DynamicTableSink actualSink = createTableSink(SCHEMA, getBasicSinkOptions());

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                new EncodingFormatMock(",");

        final DynamicTableSink expectedSink = createExpectedSink(
                SCHEMA_DATA_TYPE,
                null,
                valueEncodingFormat,
                new int[0],
                new int[]{0, 1, 2},
                null,
                TOPIC,
                PULSAR_SINK_PROPERTIES,
                PulsarSinkSemantic.AT_LEAST_ONCE,
                null,
                TestFormatFactory.IDENTIFIER
        );
        assertEquals(expectedSink, actualSink);

        // Test pulsar producer.
        final PulsarDynamicTableSink actualPulsarSink = (PulsarDynamicTableSink) actualSink;
        DynamicTableSink.SinkRuntimeProvider provider =
                actualPulsarSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkFunctionProvider.class));
        final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
        final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
        assertThat(sinkFunction, instanceOf(FlinkPulsarSink.class));
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testInvalidScanStartupMode() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Invalid value for option 'scan.startup.mode'. "
                + "Supported values are [earliest, latest, specific-offsets, external-subscription], "
                + "but was: abc")));

        final Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> options.put("scan.startup.mode", "abc"));

        createTableSource(SCHEMA, modifiedOptions);
    }

    @Test
    public void testSourceTableWithTopicAndTopicPattern() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(
                new ValidationException("Option 'topic' and 'topic-pattern' shouldn't be set together.")));

        final Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.put("topic", TOPICS);
                    options.put("topic-pattern", TOPIC_REGEX);
                });

        createTableSource(SCHEMA, modifiedOptions);
    }

    @Test
    public void testMissingSpecificOffsets() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'scan.startup.specific-offsets' "
                + "is required in 'specific-offsets' startup mode but missing.")));

        final Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> options.remove("scan.startup.specific-offsets"));

        createTableSource(SCHEMA, modifiedOptions);
    }

    @Test
    public void testInvalidSinkMessageRouter() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Could not find and instantiate messageRouter "
                + "class 'fakeRouter'")));
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSinkOptions(),
                options -> options.put("sink.message-router", "fakeRouter"));

        createTableSink(SCHEMA, modifiedOptions);
    }

    @Test
    public void testValidSinkMessageRouter() {
        final Map<String, String> mockMessageRouterOptions = getModifiedOptions(
                getBasicSinkOptions(),
                options -> options.put("sink.message-router", MockMessageRouter.class.getName()));
        final Map<String, String> keyHashRouterOptions = getModifiedOptions(
                getBasicSinkOptions(),
                options -> options.put("sink.message-router", "key-hash"));
        final Map<String, String> roundRobinRouterOptions = getModifiedOptions(
                getBasicSinkOptions(),
                options -> options.put("sink.message-router", "round-robin"));
        PulsarDynamicTableSink mockMessageRouterSink =
                (PulsarDynamicTableSink) createTableSink(SCHEMA, mockMessageRouterOptions);
        assertTrue(mockMessageRouterSink.getMessageRouter() instanceof MockMessageRouter);
        PulsarDynamicTableSink keyHashMessageRouterSink =
                (PulsarDynamicTableSink) createTableSink(SCHEMA, keyHashRouterOptions);
        assertTrue(keyHashMessageRouterSink.getMessageRouter() instanceof KeyHashMessageRouterImpl);
        PulsarDynamicTableSink roundRobinMessageRouterSink =
                (PulsarDynamicTableSink) createTableSink(SCHEMA, roundRobinRouterOptions);
        assertTrue(roundRobinMessageRouterSink.getMessageRouter() == null);
    }

    @Test
    public void testInvalidSinkSemantic() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Unsupported value 'xyz' for 'sink.semantic'. "
                + "Supported values are ['at-least-once', 'exactly-once', 'none'].")));

        final Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> options.put("sink.semantic", "xyz"));

        createTableSink(SCHEMA, modifiedOptions);
    }

    @Test
    public void testSinkWithTopicListOrTopicPattern() {
        Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.put("topic", TOPICS);
                    options.put("scan.startup.mode", "earliest");
                    options.remove("specific-offsets");
                });
        final String errorMessageTemp = "Flink Pulsar sink currently only supports single topic, but got %s: %s.";

        try {
            createTableSink(SCHEMA, modifiedOptions);
        } catch (Throwable t) {
            assertEquals(
                    String.format(errorMessageTemp, "'topic'", String.format("[%s]", String.join(", ", TOPIC_LIST))),
                    t.getCause().getMessage());
        }

        modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> options.put("topic-pattern", TOPIC_REGEX));

        try {
            createTableSink(SCHEMA, modifiedOptions);
        } catch (Throwable t) {
            assertEquals(String.format(errorMessageTemp, "'topic-pattern'", TOPIC_REGEX), t.getCause().getMessage());
        }
    }

    @Test
    public void testPrimaryKeyValidation() {
        final ResolvedSchema pkSchema =
                new ResolvedSchema(
                        SCHEMA.getColumns(),
                        SCHEMA.getWatermarkSpecs(),
                        UniqueConstraint.primaryKey(NAME, Collections.singletonList(NAME)));

        Map<String, String> options1 = getModifiedOptions(
                getBasicSourceOptions(),
                options ->
                        options.put(
                                String.format(
                                        "%s.%s",
                                        TestFormatFactory.IDENTIFIER,
                                        TestFormatFactory.CHANGELOG_MODE.key()),
                                "I;UA;UB;D"));
        // pk can be defined on cdc table, should pass
        createTableSink(pkSchema, options1);
        createTableSink(pkSchema, options1);

        try {
            createTableSink(pkSchema, getBasicSinkOptions());
            fail();
        } catch (Throwable t) {
            String error = "The Pulsar table 'default.default.t1' with 'test-format' format" +
                    " doesn't support defining PRIMARY KEY constraint on the table, because it can't" +
                    " guarantee the semantic of primary key.";
            assertEquals(error, t.getCause().getMessage());
        }

        try {
            createTableSource(pkSchema, getBasicSinkOptions());
            fail();
        } catch (Throwable t) {
            String error = "The Pulsar table 'default.default.t1' with 'test-format' format" +
                    " doesn't support defining PRIMARY KEY constraint on the table, because it can't" +
                    " guarantee the semantic of primary key.";
            assertEquals(error, t.getCause().getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static PulsarDynamicTableSource createExpectedScanSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String serviceUrl,
            String adminUrl,
            @Nullable List<String> topics,
            String topicPattern,
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

    private static PulsarDynamicTableSink createExpectedSink(
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            PulsarSinkSemantic semantic,
            @Nullable Integer parallelism,
            String formatType) {
        return new PulsarDynamicTableSink(
                SERVICE_URL,
                ADMIN_URL,
                topic,
                physicalDataType,
                properties,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                semantic,
                formatType,
                false,
                parallelism,
                KeyHashMessageRouterImpl.INSTANCE);
    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private static Map<String, String> getModifiedOptions(
            Map<String, String> options,
            Consumer<Map<String, String>> optionModifier) {
        optionModifier.accept(options);
        return options;
    }

    private static Map<String, String> getBasicSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pulsar specific options.
        tableOptions.put("connector", PulsarDynamicTableFactory.IDENTIFIER);
        tableOptions.put("generic", Boolean.TRUE.toString());
        tableOptions.put("topic", TOPIC);
        tableOptions.put("service-url", SERVICE_URL);
        tableOptions.put("admin-url", ADMIN_URL);
        tableOptions.put("scan.startup.mode", "specific-offsets");
        tableOptions.put("properties.pulsar.reader.readername", "testReaderName");
        tableOptions.put("scan.startup.specific-offsets", PROPS_SCAN_OFFSETS);
        tableOptions.put("partition.discovery.interval-millis", DISCOVERY_INTERVAL);
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        final String failOnMissingKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
        tableOptions.put(formatDelimiterKey, ",");
        tableOptions.put(failOnMissingKey, "true");
        return tableOptions;
    }

    private static Map<String, String> getBasicSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pulsar specific options.
        tableOptions.put("connector", PulsarDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic", TOPIC);
        tableOptions.put("service-url", SERVICE_URL);
        tableOptions.put("admin-url", ADMIN_URL);
        tableOptions.put("sink.semantic", PulsarTableOptions.SINK_SEMANTIC_VALUE_AT_LEAST_ONCE);
        tableOptions.put("sink.message-router", PulsarTableOptions.SINK_MESSAGE_ROUTER_VALUE_KEY_HASH);
        tableOptions.put("properties.pulsar.reader.readername", "testReaderName");

        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        tableOptions.put(formatDelimiterKey, ",");
        return tableOptions;
    }

    private static Map<String, String> getKeyValueOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pulsar specific options.
        tableOptions.put("connector", PulsarDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic", TOPIC);
        tableOptions.put("service-url", SERVICE_URL);
        tableOptions.put("admin-url", ADMIN_URL);
        tableOptions.put("partition.discovery.interval-millis", DISCOVERY_INTERVAL);
        tableOptions.put("properties.pulsar.reader.readername", "testReaderName");
        tableOptions.put("sink.semantic", PulsarTableOptions.SINK_SEMANTIC_VALUE_EXACTLY_ONCE);
        // Format options.
        tableOptions.put("key.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "#");
        tableOptions.put("key.fields", NAME);
        tableOptions.put("value.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "|");
        tableOptions.put("value.fields-include", PulsarTableOptions.ValueFieldsStrategy.EXCEPT_KEY.toString());
        return tableOptions;
    }

    /**
     * MockMessageRouter that always return partition 0 for test.
     */
    public static class MockMessageRouter implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            return 0;
        }
    }
}
