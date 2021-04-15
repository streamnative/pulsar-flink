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

import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Write Pojo class to Flink.
 *
 * @param <T> Type of the Pojo class.
 */
public class FlinkPulsarSink<T> extends FlinkPulsarSinkBase<T> {

    private final Class<T> recordClazz;

    /**
     * Type for serialized messages, default use AVRO.
     */
    private final RecordSchemaType schemaType;

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            TopicKeyExtractor<T> topicKeyExtractor,
            Class<T> recordClazz,
            RecordSchemaType recordSchemaType) {
        super(adminUrl, defaultTopicName, clientConf, properties, topicKeyExtractor);
        this.recordClazz = recordClazz;
        this.schemaType = recordSchemaType;
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            TopicKeyExtractor<T> topicKeyExtractor,
            Class<T> recordClazz) {
        this(adminUrl, defaultTopicName, clientConf, properties, topicKeyExtractor, recordClazz, RecordSchemaType.AVRO);
    }

    public FlinkPulsarSink(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            TopicKeyExtractor<T> topicKeyExtractor,
            Class<T> recordClazz) {
        this(adminUrl,
                defaultTopicName,
                PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
                properties,
                topicKeyExtractor,
                recordClazz,
                RecordSchemaType.AVRO);
    }

    public FlinkPulsarSink(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            TopicKeyExtractor<T> topicKeyExtractor,
            Class<T> recordClazz,
            RecordSchemaType recordSchemaType) {
        this(adminUrl,
                defaultTopicName,
                PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
                properties,
                topicKeyExtractor,
                recordClazz,
                recordSchemaType);
    }

    @Override
    protected Schema<T> getPulsarSchema() {
        return buildSchema(recordClazz, schemaType);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkErroneous();
        initializeSendCallback();

        TypedMessageBuilder<T> mb;

        if (forcedTopic) {
            mb = (TypedMessageBuilder<T>) getProducer(defaultTopic).newMessage().value(value);
        } else {
            byte[] key = topicKeyExtractor.serializeKey(value);
            String topic = topicKeyExtractor.getTopic(value);

            if (topic == null) {
                if (failOnWrite) {
                    throw new NullPointerException("no topic present in the data.");
                }
                return;
            }

            mb = (TypedMessageBuilder<T>) getProducer(topic).newMessage().value(value);
            if (key != null) {
                mb.keyBytes(key);
            }
        }

        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }
        mb.sendAsync().whenComplete(sendCallback);
    }

    private Schema<T> buildSchema(Class<T> recordClazz, RecordSchemaType recordSchemaType) {
        if (recordSchemaType == null) {
            return Schema.AVRO(recordClazz);
        }
        switch (recordSchemaType) {
            case AVRO:
                return Schema.AVRO(recordClazz);
            case JSON:
                return Schema.JSON(recordClazz);
            default:
                throw new IllegalArgumentException("not support schema type " + recordSchemaType);
        }
    }

}
