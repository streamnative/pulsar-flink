package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchema;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serializable;

public class PulsarRowDeserializationSchema implements PulsarDeserializationSchema<Row> {
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<Row> valueDeserialization;

    private final boolean hasMetadata;

    private final ReadableRowMetadataConverter[] metadataConverters;

    private final TypeInformation<Row> producedTypeInfo;

    PulsarRowDeserializationSchema(
            DeserializationSchema<Row> valueDeserialization,
            boolean hasMetadata,
            ReadableRowMetadataConverter[] metadataConverters,
            TypeInformation<Row> producedTypeInfo) {
        this.hasMetadata = hasMetadata;
        this.valueDeserialization = valueDeserialization;
        this.producedTypeInfo = producedTypeInfo;
        this.metadataConverters = metadataConverters;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        valueDeserialization.open(context);
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public Row deserialize(Message record) throws IOException {
        // shortcut if no metadata is required
        if (!hasMetadata) {
            return valueDeserialization.deserialize(record.getData());
        } else {
            Row physicalRow = valueDeserialization.deserialize(record.getData());
            final int physicalArity = physicalRow.getArity();
            final int metadataArity = metadataConverters.length;

            Row producedRow = new Row(physicalRow.getKind(), physicalArity + metadataArity);
            for (int i = 0; i < physicalArity; i++) {
                producedRow.setField(i, physicalRow.getField(i));
            }

            for (int i = 0; i < metadataArity; i++) {
                producedRow.setField(i + physicalArity, metadataConverters[i].read(record));
            }
            return producedRow;
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        if(hasMetadata) return producedTypeInfo;
        else return valueDeserialization.getProducedType();
    }

    // --------------------------------------------------------------------------------------------

    interface ReadableRowMetadataConverter extends Serializable {
        Object read(Message record);
    }
}
