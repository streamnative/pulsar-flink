/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarContextAware;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializationSchema;

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

    //private final Class<T> recordClazz;

    /**
     * Type for serialized messages, default use AVRO.
     */
    //private final RecordSchemaType schemaType;

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

        //get topic for message
        String targetTopic = null;
        byte[] key = null;
        if (serializationSchema instanceof PulsarContextAware) {
            @SuppressWarnings("unchecked")
            PulsarContextAware<T> contextAwareSchema =
                    (PulsarContextAware<T>) serializationSchema;

            targetTopic = contextAwareSchema.getTargetTopic(value);
            key = contextAwareSchema.getKey(value);
        }
        if (targetTopic == null) {
            targetTopic = defaultTopic;
        }

        //serialize the message
        TypedMessageBuilder<byte[]> mb = getProducer(targetTopic).newMessage();
        serializationSchema.serialize(value, mb);

        if (key != null) {
            mb.keyBytes(key);
        }

        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }
        mb.sendAsync().whenComplete(sendCallback);
    }
}
