package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarContextAware;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
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

    private final SerializableFunction<Row, String> topicExtractor;

    private int[] partitions;

    private int parallelInstanceId;

    private int numParallelInstances;

    PulsarRowSerializationSchema(
            String topic,
            SerializableFunction<Row, String> topicExtractor,
            SerializationSchema<Row> valueSerialization,
            boolean hasMetadata,
            int[] metadataPositions,
            int [] physicalPos,
            RecordSchemaType recordSchemaType,
            DataType dataType) {
        this.topic = topic;
        this.topicExtractor = topicExtractor;
        this.valueSerialization = valueSerialization;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.physicalPos = physicalPos;
        this.recordSchemaType = recordSchemaType;
        this.dataType = dataType;
    }

    PulsarRowSerializationSchema(
            String topic,
            SerializationSchema<Row> valueSerialization,
            boolean hasMetadata,
            int[] metadataPositions,
            int [] physicalPos,
            RecordSchemaType recordSchemaType,
            DataType dataType) {
        this(topic, null, valueSerialization, hasMetadata, metadataPositions, physicalPos, recordSchemaType, dataType);
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
        return SchemaUtils.buildRowSchema(dataType, recordSchemaType);
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
        if(topicExtractor == null) return topic;
        else return topicExtractor.apply(element);
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
