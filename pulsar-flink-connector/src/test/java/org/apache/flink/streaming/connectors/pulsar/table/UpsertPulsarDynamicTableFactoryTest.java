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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.api.MessageRouter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link UpsertPulsarDynamicTableFactory}.
 */
public class UpsertPulsarDynamicTableFactoryTest extends TestLogger {

    private static final String SOURCE_TOPIC = "sourceTopic_1";

    private static final String SINK_TOPIC = "sinkTopic";

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";

    private static final String ADMIN_URL = "http://127.0.0.1:8080";

    private static final ResolvedSchema SOURCE_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("window_start", DataTypes.STRING().notNull()),
                            Column.physical("region", DataTypes.STRING().notNull()),
                            Column.physical("view_count", DataTypes.BIGINT())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("window_start", "region")));


    private static final int[] SOURCE_KEY_FIELDS = new int[]{0, 1};

    private static final int[] SOURCE_VALUE_FIELDS = new int[]{2};

    private static final ResolvedSchema SINK_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(
                                    "region", new AtomicDataType(new VarCharType(false, 100))),
                            Column.physical("view_count", DataTypes.BIGINT())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Collections.singletonList("region")));

    private static final int[] SINK_KEY_FIELDS = new int[]{0};

    private static final int[] SINK_VALUE_FIELDS = new int[]{1};

    private static final Properties UPSERT_PULSAR_SOURCE_PROPERTIES = new Properties();
    private static final Properties UPSERT_PULSAR_SINK_PROPERTIES = new Properties();

    static {
//        UPSERT_PULSAR_SOURCE_PROPERTIES.setProperty("admin-url", ADMIN_URL);
//        UPSERT_PULSAR_SOURCE_PROPERTIES.setProperty("service-url", SERVICE_URL);
//
//        UPSERT_PULSAR_SINK_PROPERTIES.setProperty("admin-url", ADMIN_URL);
//        UPSERT_PULSAR_SINK_PROPERTIES.setProperty("service-url", SERVICE_URL);
    }

    protected static DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
            new TestFormatFactory.DecodingFormatMock(
                    ",", true, ChangelogMode.insertOnly(), Collections.emptyMap());

    protected static DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
            new TestFormatFactory.DecodingFormatMock(
                    ",", true, ChangelogMode.insertOnly(), Collections.emptyMap());

    protected static EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
            new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
    protected static EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
            new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testTableSource() {
        final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();
        // Construct table source using options and table source factory
        final DynamicTableSource actualSource = createTableSource(SOURCE_SCHEMA, getFullSourceOptions());

        final PulsarDynamicTableSource expectedSource = createExpectedScanSource(
                producedDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                SOURCE_KEY_FIELDS,
                SOURCE_VALUE_FIELDS,
                null,
                SOURCE_TOPIC,
                UPSERT_PULSAR_SOURCE_PROPERTIES);
        assertEquals(actualSource, expectedSource);

        final PulsarDynamicTableSource actualUpsertPulsarSource = (PulsarDynamicTableSource) actualSource;
        ScanTableSource.ScanRuntimeProvider provider =
                actualUpsertPulsarSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider, instanceOf(SourceFunctionProvider.class));
        final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
        final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
        assertThat(sourceFunction, instanceOf(FlinkPulsarSource.class));
    }

    @Test
    public void testTableSink() {

        // Construct table sink using options and table sink factory.
        final DynamicTableSink actualSink = createTableSink(SINK_SCHEMA, getFullSinkOptions());

        final DynamicTableSink expectedSink = createExpectedSink(
                SINK_SCHEMA.toPhysicalRowDataType(),
                keyEncodingFormat,
                valueEncodingFormat,
                SINK_KEY_FIELDS,
                SINK_VALUE_FIELDS,
                null,
                SINK_TOPIC,
                UPSERT_PULSAR_SINK_PROPERTIES,
                null,
                TestFormatFactory.IDENTIFIER,
                KeyHashMessageRouterImpl.INSTANCE);

        // Test sink format.
        final PulsarDynamicTableSink actualUpsertPulsarSink = (PulsarDynamicTableSink) actualSink;
        assertEquals(expectedSink, actualSink);

        // Test Pulsar producer.
        DynamicTableSink.SinkRuntimeProvider provider =
                actualUpsertPulsarSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkFunctionProvider.class));
        final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
        final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
        assertThat(sinkFunction, instanceOf(FlinkPulsarSink.class));
    }

    @Test
    public void testTableSinkWithParallelism() {
        final Map<String, String> modifiedOptions = getModifiedOptions(
                getFullSinkOptions(),
                options -> options.put("sink.parallelism", "100"));
        final DynamicTableSink actualSink = createTableSink(SINK_SCHEMA, modifiedOptions);

        final DynamicTableSink expectedSink = createExpectedSink(
                SINK_SCHEMA.toPhysicalRowDataType(),
                keyEncodingFormat,
                valueEncodingFormat,
                SINK_KEY_FIELDS,
                SINK_VALUE_FIELDS,
                null,
                SINK_TOPIC,
                UPSERT_PULSAR_SINK_PROPERTIES,
                100,
                TestFormatFactory.IDENTIFIER,
                KeyHashMessageRouterImpl.INSTANCE);
        assertEquals(expectedSink, actualSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkFunctionProvider.class));
        final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
        assertTrue(sinkFunctionProvider.getParallelism().isPresent());
        assertEquals(100, (long) sinkFunctionProvider.getParallelism().get());
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testCreateSourceTableWithoutPK() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'upsert-pulsar' tables require to define a PRIMARY KEY constraint. " +
                "The PRIMARY KEY specifies which columns should be read from or write to the Pulsar message key. " +
                "The PRIMARY KEY also defines records in the 'upsert-pulsar' table should update or delete on which keys.")));

        ResolvedSchema illegalSchema =
                ResolvedSchema.of(
                        Column.physical("window_start", DataTypes.STRING()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("view_count", DataTypes.BIGINT()));
        createTableSource(illegalSchema, getFullSourceOptions());
    }

    @Test
    public void testCreateSinkTableWithoutPK() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("'upsert-pulsar' tables require to define a PRIMARY KEY constraint. " +
                "The PRIMARY KEY specifies which columns should be read from or write to the Pulsar message key. " +
                "The PRIMARY KEY also defines records in the 'upsert-pulsar' table should update or delete on which keys.")));

        ResolvedSchema illegalSchema =
                ResolvedSchema.of(
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("view_count", DataTypes.BIGINT()));
        createTableSink(illegalSchema, getFullSinkOptions());
    }

    @Test
    public void testSerWithCDCFormatAsValue() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(
                new ValidationException(String.format(
                        "'upsert-Pulsar' connector doesn't support '%s' as value format, " +
                                "because '%s' is not in insert-only mode.",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.IDENTIFIER
                ))));

        createTableSink(SINK_SCHEMA,
                getModifiedOptions(
                        getFullSinkOptions(),
                        options -> options.put(
                                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
                                "I;UA;UB;D")));
    }

    @Test
    public void testDeserWithCDCFormatAsValue() {
        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(
                new ValidationException(String.format(
                        "'upsert-Pulsar' connector doesn't support '%s' as value format, " +
                                "because '%s' is not in insert-only mode.",
                        TestFormatFactory.IDENTIFIER,
                        TestFormatFactory.IDENTIFIER
                ))));

        createTableSource(SOURCE_SCHEMA,
                getModifiedOptions(
                        getFullSinkOptions(),
                        options -> options.put(
                                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
                                "I;UA;UB;D")));
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

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
        // table options
        Map<String, String> options = new HashMap<>();
        options.put("connector", UpsertPulsarDynamicTableFactory.IDENTIFIER);
        options.put("topic", SOURCE_TOPIC);
        options.put("admin-url", ADMIN_URL);
        options.put("service-url", SERVICE_URL);
        // key format options
        options.put("key.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
        options.put(
                String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key()), "true");
        options.put(
                String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I");
        // value format options
        options.put("value.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
        options.put(
                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key()), "true");
        options.put(
                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I");
        return options;
    }

    private static Map<String, String> getFullSinkOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", UpsertPulsarDynamicTableFactory.IDENTIFIER);
        options.put("topic", SINK_TOPIC);
        options.put("service-url", SERVICE_URL);
        options.put("admin-url", ADMIN_URL);
        // key format options
        options.put("value.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
        options.put(
                String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I"
        );
        // value format options
        options.put("key.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
        options.put(
                String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I"
        );
        return options;
    }

    private static PulsarDynamicTableSource createExpectedScanSource(
            DataType producedDataType,
            DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyFields,
            int[] valueFields,
            String keyPrefix,
            String topic,
            Properties properties) {
        PulsarTableOptions.StartupOptions startupOptions = new PulsarTableOptions.StartupOptions();
        startupOptions.startupMode = StartupMode.EARLIEST;
        startupOptions.specificOffsets = Collections.EMPTY_MAP;
        return new PulsarDynamicTableSource(
                producedDataType,
                keyDecodingFormat,
                new UpsertPulsarDynamicTableFactory.DecodingFormatWrapper(valueDecodingFormat),
                keyFields,
                valueFields,
                keyPrefix,
                Collections.singletonList(topic),
                null,
                SERVICE_URL,
                ADMIN_URL,
                properties,
                startupOptions,
                true);
    }

    private static PulsarDynamicTableSink createExpectedSink(
            DataType consumedDataType,
            EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            String keyPrefix,
            String topic,
            Properties properties,
            Integer parallelism,
            String formatType,
            MessageRouter messageRouter) {
        return new PulsarDynamicTableSink(
                SERVICE_URL,
                ADMIN_URL,
                topic,
                consumedDataType,
                properties,
                keyEncodingFormat,
                new UpsertPulsarDynamicTableFactory.EncodingFormatWrapper(valueEncodingFormat),
                keyProjection,
                valueProjection,
                keyPrefix,
                PulsarSinkSemantic.AT_LEAST_ONCE,
                formatType,
                true,
                parallelism,
                messageRouter);
    }
}
