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
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;

import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Write data to Flink.
 *
 * @param <T> Type of the Pojo or RowData class.
 */
public class FlinkPulsarSink<T> extends FlinkPulsarSinkBase<T> {

    private final PulsarSerializationSchema<T> serializationSchema;

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema serializationSchema) {

        super(adminUrl, defaultTopicName, clientConf, properties, serializationSchema);
        this.serializationSchema = serializationSchema;
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
                serializationSchema
        );
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkErroneous();
        initializeSendCallback();
        final Optional<String> targetTopic = serializationSchema.getTargetTopic(value);
        String topic = targetTopic.orElse(defaultTopic);
        TypedMessageBuilder<T> mb = getProducer(topic).newMessage();
        serializationSchema.serialize(value, mb);

        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }
        mb.sendAsync().whenComplete(sendCallback);
    }
}
