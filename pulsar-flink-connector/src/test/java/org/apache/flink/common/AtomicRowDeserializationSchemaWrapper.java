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

package org.apache.flink.common;

import org.apache.flink.formats.atomic.AtomicRowDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * an util wrapper for AtomicRowDeserializationSchema.
 */
public class AtomicRowDeserializationSchemaWrapper extends PulsarDeserializationSchemaWrapper<Row> {

    private final AtomicRowDeserializationSchema atomicRowDeserializationSchema;

    public AtomicRowDeserializationSchemaWrapper(AtomicRowDeserializationSchema deserializationSchema) {
        super(deserializationSchema);
        this.atomicRowDeserializationSchema = deserializationSchema;
    }

    @Override
    public Row deserialize(Message message) throws IOException {
        Row origin = super.deserialize(message);
        if (atomicRowDeserializationSchema.isUseExtendFields()) {
            //extract meta data
            return useMetaData(origin, message);
        }
        return origin;
    }

    private Row useMetaData(Row origin, Message message) {
        Row resultRow = new Row(origin.getArity() + PulsarOptions.META_FIELD_NAMES.size());
        for (int i = 0; i < origin.getArity(); i++) {
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
