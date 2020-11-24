package org.apache.flink.common;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.atomic.AtomicRowDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class AtomicRowDeserializationSchemaWrapper extends PulsarDeserializationSchemaWrapper<Row> {

    private final AtomicRowDeserializationSchema atomicRowDeserializationSchema;
    public AtomicRowDeserializationSchemaWrapper(AtomicRowDeserializationSchema deserializationSchema) {
        super(deserializationSchema);
        this.atomicRowDeserializationSchema = deserializationSchema;
    }

    @Override
    public Row deserialize(Message message) throws IOException {
        Row origin = super.deserialize(message);
        if(atomicRowDeserializationSchema.isUseExtendFields()){
            //extract meta data
            return useMetaData(origin, message);
        }
        return origin;
    }

    private Row useMetaData(Row origin, Message message){
        Row resultRow = new Row(origin.getArity() + PulsarOptions.META_FIELD_NAMES.size());
        for(int i = 0; i < origin.getArity(); i++){
            resultRow.setField(i, origin.getField(i));
        }
        int metaStartIdx = origin.getArity();
        if (message.hasKey()) {
            resultRow.setField(metaStartIdx, message.getKeyBytes());
        } else {
            resultRow.setField(metaStartIdx, null);
        }

        resultRow.setField(metaStartIdx + 1, message.getTopicName());
        resultRow.setField(metaStartIdx + 2, message.getMessageId().toByteArray());
        resultRow.setField(metaStartIdx + 3, LocalDateTime.ofInstant(Instant.ofEpochMilli(message.getPublishTime()), ZoneId.systemDefault()));

        if (message.getEventTime() > 0L) {
            resultRow.setField(metaStartIdx + 4, LocalDateTime.ofInstant(Instant.ofEpochMilli(message.getEventTime()), ZoneId.systemDefault()));
        } else {
            resultRow.setField(metaStartIdx + 4, null);
        }
        return resultRow;
    }

}
