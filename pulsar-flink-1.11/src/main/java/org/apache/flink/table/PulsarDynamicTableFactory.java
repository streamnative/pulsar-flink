package org.apache.flink.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Factory for creating configured instances of
 * {@link PulsarDynamicTableFactory}.
 */
public class PulsarDynamicTableFactory  implements
        DynamicTableSourceFactory,
        DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        String topic = tableOptions.get(PulsarOptions.TOPIC);
        String adminUrl = tableOptions.get(PulsarOptions.TOPIC);
        String serverUrl = tableOptions.get(PulsarOptions.TOPIC);
        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PulsarOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        PulsarOptions.validateTableOptions(tableOptions);

        DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new PulsarDynamicTableSink(
                serverUrl,
                adminUrl,
                topic,
                consumedDataType,
                new Properties(),
                encodingFormat
                );
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        String topic = tableOptions.get(PulsarOptions.TOPIC);
        String adminUrl = tableOptions.get(PulsarOptions.ADMIN_URL);
        String serviceUrl = tableOptions.get(PulsarOptions.SERVICE_URL);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PulsarOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        PulsarOptions.validateTableOptions(tableOptions);

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        final PulsarOptions.StartupOptions startupOptions = PulsarOptions.getStartupOptions(tableOptions, topic);
        return new PulsarDynamicTableSource(
                producedDataType,
                decodingFormat,
                topic,
                serviceUrl,
                adminUrl,
                new Properties(),
                startupOptions
        );
    }

    @Override
    public String factoryIdentifier() {
        return "pulsar";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PulsarOptions.TOPIC);
        options.add(FactoryUtil.FORMAT);
        options.add(PulsarOptions.SERVICE_URL);
        options.add(PulsarOptions.ADMIN_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PulsarOptions.SCAN_STARTUP_MODE);
        options.add(PulsarOptions.SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(PulsarOptions.SCAN_STARTUP_SUB_NAME);

        options.add(PulsarOptions.PULSAR_READER_READER_NAME);
        options.add(PulsarOptions.PULSAR_READER_SUBSCRIPTION_ROLE_PREFIX);
        options.add(PulsarOptions.PULSAR_READER_RECEIVER_QUEUE_SIZE);
        options.add(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MILLIS);

        return options;
    }
}
