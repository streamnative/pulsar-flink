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

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class RowReaderThread extends ReaderThread<Row> {

    private final Schema<?> schema;

    private final PulsarDeserializer deserializer;

    public RowReaderThread(
            PulsarFetcher owner,
            PulsarTopicState state,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            int pollTimeoutMs,
            SchemaInfo pulsarSchema,
            JSONOptionsInRead jsonOptions,
            ExceptionProxy exceptionProxy) {

        super(owner, state, clientConf, readerConf, null, pollTimeoutMs, exceptionProxy);
        this.schema = SchemaUtils.getPulsarSchema(pulsarSchema);
        this.deserializer = new PulsarDeserializer(pulsarSchema, jsonOptions);
    }

    @Override
    protected void createActualReader() throws PulsarClientException, ExecutionException {
        reader = CachedPulsarClient
            .getOrCreate(clientConf)
            .newReader(schema)
            .topic(topic)
            .startMessageId(startMessageId)
            .startMessageIdInclusive()
            .loadConf(readerConf)
            .create();
    }

    @Override
    protected void emitRecord(Message<?> message) throws IOException {
        val messageId = message.getMessageId();
        val record = deserializer.deserialize(message);
        owner.emitRecord(record, state, messageId);
    }
}
