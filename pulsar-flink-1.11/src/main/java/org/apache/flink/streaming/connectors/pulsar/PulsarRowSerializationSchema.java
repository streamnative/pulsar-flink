package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarContextAware;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;

public class PulsarRowSerializationSchema implements PulsarSerializationSchema<Row>, PulsarContextAware<Row> {
    private static final long serialVersionUID = 1L;

    private final String topic;

    private final SerializationSchema<Row> valueSerialization;

    private final boolean hasMetadata;

    /**
     * Contains the position for each value of {@link org.apache.flink.streaming.connectors.pulsar.PulsarTableSink.WritableMetadata} in the consumed row or
     * -1 if this metadata key is not used.
     */
    private final int[] metadataPositions;

    private final int[] physicalPos;

    private final RecordSchemaType recordSchemaType;

    private final DataType dataType;

    private int[] partitions;

    private int parallelInstanceId;

    private int numParallelInstances;

    PulsarRowSerializationSchema(
            String topic,
            SerializationSchema<Row> valueSerialization,
            boolean hasMetadata,
            int[] metadataPositions,
            int [] physicalPos,
            RecordSchemaType recordSchemaType,
            DataType dataType) {
        this.topic = topic;
        this.valueSerialization = valueSerialization;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.physicalPos = physicalPos;
        this.recordSchemaType = recordSchemaType;
        this.dataType = dataType;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        valueSerialization.open(context);
    }

    @Override
    public byte[] serialize(Row element) {
        throw new IllegalStateException("In Row mode we use serialize method with 2 args");
    }

    @Override
    public void serialize(Row consumedRow, TypedMessageBuilder<byte[]> messageBuilder) {
        final Row physicalRow;
        // shortcut if no metadata is required
        if (!hasMetadata) {
            physicalRow = consumedRow;
        } else {
            final int physicalArity = physicalPos.length;
            Row row = new Row(consumedRow.getKind(), physicalArity);
            for (int i = 0; i < physicalArity; i++) {
                row.setField(i, consumedRow.getField(physicalPos[i]));
            }
            physicalRow = row;
        }

        final byte[] valueSerialized = valueSerialization.serialize(physicalRow);
        messageBuilder.value(valueSerialized);
    }

    @Override
    public Schema<?> getPulsarSchema() {
        org.apache.avro.Schema avroSchema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        byte[] schemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
        SchemaInfo si = new SchemaInfo();
        si.setSchema(schemaBytes);
        //String formatName = properties.getProperty(FormatDescriptorValidator.FORMAT_TYPE, JsonFormatFactory.IDENTIFIER);
        switch (recordSchemaType) {
            case AVRO:
                si.setName("Avro");
                si.setType(SchemaType.AVRO);
                break;
            case JSON:
                si.setName("Json");
                si.setType(SchemaType.JSON);
                break;
            case ATOMIC:
                try {
                    return SimpleSchemaTranslator.atomicType2PulsarSchema(dataType);
                } catch (IncompatibleSchemaException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new IllegalStateException("for now we just support json、avro、atomic format for rowData");
        }
        return Schema.generic(si);

    }

    @Override
    public void setParallelInstanceId(int parallelInstanceId) {
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public void setNumParallelInstances(int numParallelInstances) {
        this.numParallelInstances = numParallelInstances;
    }

    @Override
    public void setPartitions(int[] partitions) {
        this.partitions = partitions;
    }

    @Override
    public String getTargetTopic(Row element) {
        return topic;
    }

    @Override
    public byte[] getKey(Row element) {
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(Row consumedRow, PulsarTableSink.WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }

    interface WritableRowMetadataConverter{
        Object read(Row consumedRow, int pos);
    }

}
