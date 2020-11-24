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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.types.Row;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Specific reader thread to read flink rows from a Pulsar partition.
 */
@Slf4j
public class RowReaderThread extends ReaderThread<Row> {

    private final Schema<?> schema;
    private final boolean useExtendField;
    public RowReaderThread(
            PulsarFetcher owner,
            PulsarTopicState state,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            int pollTimeoutMs,
            SchemaInfo pulsarSchema,
            PulsarDeserializationSchema<Row> deserializer,
            ExceptionProxy exceptionProxy,
            boolean useExtendField) {
        super(owner, state, clientConf, readerConf,
                deserializer,
                pollTimeoutMs, exceptionProxy);
        this.schema = SchemaUtils.getPulsarSchema(pulsarSchema);
        this.useExtendField = useExtendField;
    }

    @Override
    protected void createActualReader() throws PulsarClientException, ExecutionException {
        final ReaderBuilder<?> readerBuilder = CachedPulsarClient
                .getOrCreate(clientConf)
                .newReader(schema)
                .topic(topicRange.getTopic())
                .startMessageId(startMessageId)
                .startMessageIdInclusive()
                .loadConf(readerConf);
        if (!topicRange.isFullRange()) {
            readerBuilder.keyHashRange(topicRange.getPulsarRange());
        }
        reader = readerBuilder.create();
    }

    private Row useMetaData(Row origin, Message message){
        //RowUpdate fieldUpdater = new RowUpdater();
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


    @Override
    protected void emitRecord(Message<?> message) throws IOException {
        try {
            MessageId messageId = message.getMessageId();
            Row record = deserializer.deserialize(message);
            if(useExtendField){
                record = useMetaData(record, message);
            }
            if (deserializer.isEndOfStream(record)) {
                return;
            }
            if (record.getField(0) == null){
                throw new RuntimeException("record index 0 is null");
            }
            owner.emitRecord(record, state, messageId);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }
}
