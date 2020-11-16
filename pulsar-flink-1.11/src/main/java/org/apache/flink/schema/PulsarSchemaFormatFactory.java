package org.apache.flink.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.JSONOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializer;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializer;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.common.collect.Maps;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

import java.util.Optional;
import java.util.Set;

import static org.apache.flink.schema.PulsarSchemaOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.schema.PulsarSchemaOptions.JSON_OPTIONS;
import static org.apache.flink.schema.PulsarSchemaOptions.PULSAR_SCHEMA;

public class PulsarSchemaFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context,
                                                                               ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        final Optional<SchemaType> schemaType = formatOptions.getOptional(PULSAR_SCHEMA);
        final Optional<String> optional = formatOptions.getOptional(JSON_OPTIONS);

        // TODO param JSONOptions
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType) {
                final TypeInformation<RowData> rowDataTypeInfo =
                        (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
                final JSONOptions parsedOptions = new JSONOptions(Maps.newHashMap(), "", "");
                return  new PulsarRowDataDeserializer(
                        producedDataType,
                        schemaType.get(),
                        parsedOptions,
                        rowDataTypeInfo
                );
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context,
                                                                             ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        final Optional<SchemaType> schemaType = formatOptions.getOptional(PULSAR_SCHEMA);
        final boolean nullable = formatOptions.get(FAIL_ON_MISSING_FIELD);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context,
                    DataType consumedDataType) {
                final PulsarRowDataSerializer pulsarSerializer = new PulsarRowDataSerializer(consumedDataType,
                        nullable, schemaType.get());
                return new SerializationSchema<RowData>() {
                    @Override
                    public void open(InitializationContext context) throws Exception {
                        pulsarSerializer.open(context);
                    }

                    @Override
                    public byte[] serialize(RowData element) {
                        return pulsarSerializer.serialize(element);
                    }
                };
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    private void validateFormatOptions(ReadableConfig formatOptions) {

    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(PULSAR_SCHEMA);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(JSON_OPTIONS, FAIL_ON_MISSING_FIELD);
    }
}
