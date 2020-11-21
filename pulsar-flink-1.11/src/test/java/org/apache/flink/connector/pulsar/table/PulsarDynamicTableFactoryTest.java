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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.api.MessageId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * PulsarDynamicTableFactory test.
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
    private static final String TIME = "time";
    private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);
    private static final String DISCOVERY_INTERVAL = "1000 ms";

    private static final Properties PULSAR_SOURCE_PROPERTIES = new Properties();
    private static final Properties PULSAR_SINK_PROPERTIES = new Properties();

    static {
        PULSAR_SOURCE_PROPERTIES.setProperty("admin-url", ADMIN_URL);
        PULSAR_SOURCE_PROPERTIES.setProperty("service-url", SERVICE_URL);
        PULSAR_SOURCE_PROPERTIES.setProperty("partition.discovery.interval-millis", "1000");

        PULSAR_SINK_PROPERTIES.setProperty("admin-url", ADMIN_URL);
        PULSAR_SINK_PROPERTIES.setProperty("service-url", SERVICE_URL);
    }

    private static final String PROPS_SCAN_OFFSETS =
            MessageId.earliest.toString();

    private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
            .field(NAME, DataTypes.STRING())
            .field(COUNT, DataTypes.DECIMAL(38, 18))
            .field(TIME, DataTypes.TIMESTAMP(3))
            .field(COMPUTED_COLUMN_NAME, COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION)
            .watermark(TIME, WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
            .build();

    private static final TableSchema SINK_SCHEMA = TableSchema.builder()
            .field(NAME, DataTypes.STRING())
            .field(COUNT, DataTypes.DECIMAL(38, 18))
            .field(TIME, DataTypes.TIMESTAMP(3))
            .build();

    @Test
    public void testTableSource() {
        // prepare parameters for pulsar table source
        final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

        final Map<String, MessageId> specificOffsets = new HashMap<>();
        specificOffsets.put("-1", MessageId.earliest);
//        specificOffsets.put(TOPIC, MessageId.earliest);

        final PulsarOptions.StartupOptions startupOptions = new PulsarOptions.StartupOptions();
        startupOptions.startupMode = StartupMode.SPECIFIC_OFFSETS;
        startupOptions.specificOffsets = specificOffsets;
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                new TestFormatFactory.DecodingFormatMock(",", true);

        // Construct table source using options and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        CatalogTable catalogTable = createPulsarSourceCatalogTable();
        final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader());

        // Test scan source equals
        final PulsarDynamicTableSource expectedPulsarSource = createExpectedScanSource(
                producedDataType,
                SERVICE_URL,
                ADMIN_URL,
                Collections.singletonList(TOPIC),
                null,
                PULSAR_SOURCE_PROPERTIES,
                decodingFormat,
                startupOptions);
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
    public void testTableSourceWithPattern() {
        // prepare parameters for Pulsar table source
        final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                new TestFormatFactory.DecodingFormatMock(",", true);

        // Construct table source using options and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSourceOptions(),
                options -> {
                    options.remove("topic");
                    options.put("topic-pattern", TOPIC_REGEX);
                    options.put("scan.startup.mode", "earliest");
                    options.remove("scan.startup.specific-offsets");
                });
        CatalogTable catalogTable = createPulsarSourceCatalogTable(modifiedOptions);

        final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader()
        );

        final PulsarOptions.StartupOptions startupOptions = new PulsarOptions.StartupOptions();
        startupOptions.startupMode = StartupMode.EARLIEST;
        startupOptions.specificOffsets = new HashMap<>();
        // Test scan source equals
        final PulsarDynamicTableSource expectedPulsarSource = createExpectedScanSource(
                producedDataType,
                SERVICE_URL,
                ADMIN_URL,
                null,
                TOPIC_REGEX,
                PULSAR_SOURCE_PROPERTIES,
                decodingFormat,
                startupOptions);
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
    public void testTableSink() {
        final DataType consumedDataType = SINK_SCHEMA.toPhysicalRowDataType();
        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                new TestFormatFactory.EncodingFormatMock(",");

        // Construct table sink using options and table sink factory.
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "sinkTable");
        final CatalogTable sinkTable = createPulsarSinkCatalogTable();
        final DynamicTableSink actualSink = FactoryUtil.createTableSink(
                null,
                objectIdentifier,
                sinkTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader()
        );

        final PulsarDynamicTableSink expectedSink = createExpectedSink(
                consumedDataType,
                SERVICE_URL,
                ADMIN_URL,
                TOPIC,
                PULSAR_SINK_PROPERTIES,
                encodingFormat);
        assertEquals(expectedSink, actualSink);

        // Test sink format.
        final PulsarDynamicTableSink actualPulsarSink = (PulsarDynamicTableSink) actualSink;
        assertEquals(encodingFormat, actualPulsarSink.encodingFormat);

        // Test pulsar producer.
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
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSourceOptions(),
                options -> options.put("scan.startup.mode", "abc"));
        CatalogTable catalogTable = createPulsarSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Invalid value for option 'scan.startup.mode'. "
                + "Supported values are [earliest, latest, specific-offsets, external-subscription], "
                + "but was: abc")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader()
        );
    }

    @Test
    public void testSourceTableWithTopicAndTopicPattern() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSourceOptions(),
                options -> {
                    options.put("topic", TOPICS);
                    options.put("topic-pattern", TOPIC_REGEX);
                });
        CatalogTable catalogTable = createPulsarSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(
                new ValidationException("Option 'topic' and 'topic-pattern' shouldn't be set together.")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader()
        );
    }

    @Test
    public void testMissingSpecificOffsets() {
        // Construct table source using DDL and table source factory
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "scanTable");
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSourceOptions(),
                options -> options.remove("scan.startup.specific-offsets"));
        CatalogTable catalogTable = createPulsarSourceCatalogTable(modifiedOptions);

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'scan.startup.specific-offsets' "
                + "is required in 'specific-offsets' startup mode but missing.")));
        FactoryUtil.createTableSource(null,
                objectIdentifier,
                catalogTable,
                new Configuration(),
                Thread.currentThread().getContextClassLoader()
        );
    }

    @Test
    public void testSinkWithTopicListOrTopicPattern() {
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
                "default",
                "default",
                "sinkTable");

        Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSourceOptions(),
                options -> {
                    options.put("topic", TOPICS);
                    options.put("scan.startup.mode", "earliest");
                    options.remove("specific-offsets");
                });
        CatalogTable sinkTable = createPulsarSinkCatalogTable(modifiedOptions);
        String errorMessageTemp = "Flink Pulsar sink currently only supports single topic, but got %s: %s.";

        try {
            FactoryUtil.createTableSink(
                    null,
                    objectIdentifier,
                    sinkTable,
                    new Configuration(),
                    Thread.currentThread().getContextClassLoader()
            );
        } catch (Throwable t) {
            assertEquals(
                    String.format(errorMessageTemp, "'topic'", String.format("[%s]", String.join(", ", TOPIC_LIST))),
                    t.getCause().getMessage());
        }

        modifiedOptions = getModifiedOptions(
                getFullSourceOptions(),
                options -> options.put("topic-pattern", TOPIC_REGEX));
        sinkTable = createPulsarSinkCatalogTable(modifiedOptions);

        try {
            FactoryUtil.createTableSink(
                    null,
                    objectIdentifier,
                    sinkTable,
                    new Configuration(),
                    Thread.currentThread().getContextClassLoader()
            );
        } catch (Throwable t) {
            assertEquals(String.format(errorMessageTemp, "'topic-pattern'", TOPIC_REGEX), t.getCause().getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static PulsarDynamicTableSource createExpectedScanSource(
            DataType producedDataType,
            String serviceUrl,
            String adminUrl,
            @Nullable List<String> topics,
            String topicPattern,
            Properties properties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            PulsarOptions.StartupOptions startupOptions
    ) {

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

    private static PulsarDynamicTableSink createExpectedSink(
            DataType consumedDataType,
            String serviceUrl,
            String adminUrl,
            String topic,
            Properties properties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {

        return new PulsarDynamicTableSink(
                serviceUrl,
                adminUrl,
                topic,
                consumedDataType,
                properties,
                encodingFormat,
                PulsarOptions.getSinkSemantic(tableOptions));
    }

    private static CatalogTable createPulsarSourceCatalogTable() {
        return createPulsarSourceCatalogTable(getFullSourceOptions());
    }

    private static CatalogTable createPulsarSinkCatalogTable() {
        return createPulsarSinkCatalogTable(getFullSinkOptions());
    }

    private static CatalogTable createPulsarSourceCatalogTable(Map<String, String> options) {
        return new CatalogTableImpl(SOURCE_SCHEMA, options, "scanTable");
    }

    private static CatalogTable createPulsarSinkCatalogTable(Map<String, String> options) {
        return new CatalogTableImpl(SINK_SCHEMA, options, "sinkTable");
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

    private static Map<String, String> getFullSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pulsar specific options.
        tableOptions.put("connector", PulsarDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic", TOPIC);
        tableOptions.put("service-url", SERVICE_URL);
        tableOptions.put("admin-url", ADMIN_URL);
        tableOptions.put("scan.startup.mode", "specific-offsets");
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

    private static Map<String, String> getFullSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Pulsar specific options.
        tableOptions.put("connector", PulsarDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic", TOPIC);
        tableOptions.put("service-url", SERVICE_URL);
        tableOptions.put("admin-url", ADMIN_URL);
//        tableOptions.put("sink.partitioner", PulsarOptions.SINK_PARTITIONER_VALUE_FIXED);
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey = String.format("%s.%s",
                TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        tableOptions.put(formatDelimiterKey, ",");
        return tableOptions;
    }
}
