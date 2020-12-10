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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;

import org.apache.pulsar.client.api.MessageRouter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Write data to Flink.
 *
 * @param <T> Type of the Pojo or RowData class.
 */
@Slf4j
public class FlinkPulsarSink<T> extends FlinkPulsarSinkBase<T> {

    private final PulsarSerializationSchema<T> serializationSchema;

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema serializationSchema,
            MessageRouter messageRouter,
            PulsarSinkSemantic semantic) {

        super(adminUrl, defaultTopicName, clientConf, properties, serializationSchema, messageRouter);
        this.serializationSchema = serializationSchema;
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema serializationSchema,
            PulsarSinkSemantic semantic) {

        super(adminUrl, defaultTopicName, clientConf, properties, serializationSchema, semantic);
        this.serializationSchema = serializationSchema;
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema serializationSchema) {
        this(adminUrl, defaultTopicName, clientConf, properties, serializationSchema, null);

        this(adminUrl, defaultTopicName, clientConf, properties, serializationSchema, PulsarSinkSemantic.AT_LEAST_ONCE);
    }

    public FlinkPulsarSink(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            PulsarSerializationSchema<T> serializationSchema) {
        this(
                adminUrl,
                defaultTopicName,
                PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
                properties,
                serializationSchema,
                null
        );
    }

    @Override
    public void invoke(PulsarTransactionState<T> transactionState, T value, Context context) throws Exception {
        checkErroneous();
        initializeSendCallback();

        final Optional<String> targetTopic = serializationSchema.getTargetTopic(value);
        String topic = targetTopic.orElse(defaultTopic);

        TypedMessageBuilder<T> mb = transactionState.isTransactional() ?
                getProducer(topic).newMessage(transactionState.getTransaction()) : getProducer(topic).newMessage();
        serializationSchema.serialize(value, mb);

        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }

        CompletableFuture<MessageId> messageIdFuture = mb.sendAsync();
        if (transactionState.isTransactional()) {
            TxnID transactionalId = transactionState.transactionalId;
            List<CompletableFuture<MessageId>> futureList;
            if (tid2FuturesMap.get(transactionalId) == null) {
                futureList = new ArrayList<>();
                tid2FuturesMap.put(transactionalId, futureList);
            } else {
                futureList = tid2FuturesMap.get(transactionalId);
            }
            futureList.add(messageIdFuture);
            log.info("message {} is invoke in txn {}", value, transactionState.transactionalId);
        }
        messageIdFuture.whenComplete(sendCallback);
    }
}
