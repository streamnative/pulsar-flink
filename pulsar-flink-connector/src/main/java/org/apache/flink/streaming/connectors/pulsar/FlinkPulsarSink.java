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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.InstantiationUtil.isSerializable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Write data to Flink.
 *
 * @param <T> Type of the record.
 */
@Slf4j
public class FlinkPulsarSink<T> extends FlinkPulsarSinkBase<T> {

    public static class Builder<T> {
        private String adminUrl;
        private String defaultTopicName;
        private ClientConfigurationData clientConf;
        private Properties properties;
        private PulsarSerializationSchema<T>  serializationSchema;
        private MessageRouter messageRouter = null;
        private PulsarSinkSemantic semantic = PulsarSinkSemantic.AT_LEAST_ONCE;
        private String serviceUrl;
        private CryptoKeyReader cryptoKeyReader;
        private final Set<String> encryptionKeys = new HashSet<>();

        public Builder<T> withAdminUrl(final String adminUrl) {
            this.adminUrl = adminUrl;
            return this;
        }

        public Builder<T> withDefaultTopicName(final String defaultTopicName) {
            this.defaultTopicName = defaultTopicName;
            return this;
        }

        public Builder<T> withClientConf(final ClientConfigurationData clientConf) {
            this.clientConf = clientConf;
            return this;
        }

        public Builder<T> withProperties(final Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder<T> withPulsarSerializationSchema(final PulsarSerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        public Builder<T> withMessageRouter(final MessageRouter messageRouter) {
            this.messageRouter = messageRouter;
            return this;
        }

        public Builder<T> withSemantic(final PulsarSinkSemantic semantic) {
            this.semantic = semantic;
            return this;
        }

        public Builder<T> withServiceUrl(final String serviceUrl) {
            this.serviceUrl = serviceUrl;
            return this;
        }

        public Builder<T> withCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
            this.cryptoKeyReader = cryptoKeyReader;
            return this;
        }

        public Builder<T> withEncryptionKeys(String... encryptionKeys) {
            this.encryptionKeys.addAll(Arrays.asList(encryptionKeys));
            return this;
        }

        private Optional<String> getDefaultTopicName() {
            return Optional.ofNullable(defaultTopicName);
        }

        public FlinkPulsarSink<T>  build(){
            if (adminUrl == null) {
                throw new IllegalStateException("Admin URL must be set.");
            }
            if (serializationSchema == null) {
                throw new IllegalStateException("Serialization schema must be set.");
            }
            if (semantic == null) {
                throw new IllegalStateException("Semantic must be set.");
            }
            if (properties == null) {
                throw new IllegalStateException("Properties must be set.");
            }
            if (serviceUrl != null && clientConf != null) {
                throw new IllegalStateException("Set either client conf or service URL but not both.");
            }
            if (serviceUrl != null){
                clientConf = PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties);
            }
            if (clientConf == null){
                throw new IllegalStateException("Client conf must be set.");
            }
            if ((cryptoKeyReader == null) != (encryptionKeys.isEmpty())){
                throw new IllegalStateException("Set crypto key reader and encryption keys in conjunction.");
            }
            checkState(isSerializable(cryptoKeyReader));
            checkState(isSerializable(encryptionKeys));
            return new FlinkPulsarSink<>(this);
        }

    }

    private final PulsarSerializationSchema<T> serializationSchema;

    private FlinkPulsarSink(final Builder<T> builder) {
        super(builder.adminUrl, builder.getDefaultTopicName(), builder.clientConf, builder.properties, builder.serializationSchema, builder.messageRouter, builder.semantic, builder.cryptoKeyReader, builder.encryptionKeys);
        this.serializationSchema = builder.serializationSchema;
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema serializationSchema,
            MessageRouter messageRouter,
            PulsarSinkSemantic semantic) {
        this(new Builder<T>()
            .withAdminUrl(adminUrl)
            .withDefaultTopicName(defaultTopicName.orElse(null))
            .withClientConf(clientConf)
            .withProperties(properties)
            .withPulsarSerializationSchema(serializationSchema)
            .withMessageRouter(messageRouter)
            .withSemantic(semantic));
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema serializationSchema,
            PulsarSinkSemantic semantic) {
        this(adminUrl, defaultTopicName, clientConf, properties, serializationSchema, null, semantic);
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema serializationSchema) {
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
                PulsarSinkSemantic.AT_LEAST_ONCE
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
            // in transactional mode, we must sleep some time because pulsar have some bug can result data disorder.
            // if pulsar-client fix this bug, we can safely remove this.
            Thread.sleep(10);
            TxnID transactionalId = transactionState.transactionalId;
            List<CompletableFuture<MessageId>> futureList;
            tid2FuturesMap.computeIfAbsent(transactionalId, key -> new ArrayList<>())
                    .add(messageIdFuture);
            log.debug("message {} is invoke in txn {}", value, transactionState.transactionalId);
        }
        messageIdFuture.whenComplete(sendCallback);
    }
}
