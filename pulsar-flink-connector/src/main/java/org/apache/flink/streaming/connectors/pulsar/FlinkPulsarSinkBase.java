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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializableObject;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink Sink to produce data into Pulsar topics.
 *
 * <p>Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @param <T> Type of the messages to write into pulsar.
 */
@Slf4j
abstract class FlinkPulsarSinkBase<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    protected String adminUrl;

    protected ClientConfigurationData clientConfigurationData;

    protected final Map<String, String> caseInsensitiveParams;

    protected final Map<String, Object> producerConf;

    protected final Properties properties;

    protected boolean flushOnCheckpoint;

    protected boolean failOnWrite;

    /** Lock for accessing the pending records. */
    protected final SerializableObject pendingRecordsLock = new SerializableObject();

    /** Number of unacknowledged records. */
    protected long pendingRecords = 0L;

    protected final boolean forcedTopic;

    protected final String defaultTopic;

    protected final PulsarSerializationSchema<T> serializationSchema;

    protected transient volatile Throwable failedWrite;

    protected transient PulsarAdmin admin;

    protected transient BiConsumer<MessageId, Throwable> sendCallback;

    protected transient Producer<byte[]> singleProducer;

    protected transient Map<String, Producer<byte[]>> topic2Producer;

    public FlinkPulsarSinkBase(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSerializationSchema<T> serializationSchema) {

        this.adminUrl = checkNotNull(adminUrl);

        if (defaultTopicName.isPresent()) {
            this.forcedTopic = true;
            this.defaultTopic = defaultTopicName.get();
        } else {
            this.forcedTopic = false;
            this.defaultTopic = null;
        }

        this.serializationSchema = serializationSchema;

        this.clientConfigurationData = clientConf;

        this.properties = checkNotNull(properties);

        this.caseInsensitiveParams =
                SourceSinkUtils.toCaceInsensitiveParams(Maps.fromProperties(properties));

        this.producerConf =
                SourceSinkUtils.getProducerParams(Maps.fromProperties(properties));

        this.flushOnCheckpoint =
                SourceSinkUtils.flushOnCheckpoint(caseInsensitiveParams);

        this.failOnWrite =
                SourceSinkUtils.failOnWrite(caseInsensitiveParams);

        CachedPulsarClient.setCacheSize(SourceSinkUtils.getClientCacheSize(caseInsensitiveParams));

        if (this.clientConfigurationData.getServiceUrl() == null) {
            throw new IllegalArgumentException("ServiceUrl must be supplied in the client configuration");
        }
    }

    public FlinkPulsarSinkBase(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            PulsarSerializationSchema serializationSchema) {
        this(adminUrl,
                defaultTopicName,
                PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
                properties,
                serializationSchema);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkErroneous();

        if (flushOnCheckpoint) {
            producerFlush();
            synchronized (pendingRecordsLock) {
                if (pendingRecords != 0) {
                    throw new IllegalStateException("Pending record count must be zero at this point " + pendingRecords);
                }
                checkErroneous();
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    //protected abstract Schema<?> getPulsarSchema();

    @Override
    public void open(Configuration parameters) throws Exception {
        if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
            log.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
            flushOnCheckpoint = false;
        }

        admin = PulsarClientUtils.newAdminFromConf(adminUrl, clientConfigurationData);

        serializationSchema.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));

        if (forcedTopic) {
            uploadSchema(defaultTopic);
            singleProducer = createProducer(clientConfigurationData, producerConf, defaultTopic);
        } else {
            topic2Producer = new HashMap<>();
        }
    }

    protected void initializeSendCallback() {
        if (sendCallback != null) {
            return;
        }
        if (failOnWrite) {
            this.sendCallback = (t, u) -> {
                if (failedWrite == null && u == null) {
                    acknowledgeMessage();
                } else if (failedWrite == null && u != null) {
                    failedWrite = u;
                } else { // failedWrite != null
                    log.warn("callback error {}", u);
                    // do nothing and wait next checkForError to throw exception
                }
            };
        } else {
            this.sendCallback = (t, u) -> {
                if (failedWrite == null && u != null) {
                    log.error("Error while sending message to Pulsar: {}", ExceptionUtils.stringifyException(u));
                }
                acknowledgeMessage();
            };
        }
    }

    private void uploadSchema(String topic) {
        SchemaUtils.uploadPulsarSchema(admin, topic, serializationSchema.getPulsarSchema().getSchemaInfo());
    }

    @Override
    public void close() throws Exception {
        checkErroneous();
        producerClose();
        checkErroneous();
    }

    protected Producer<byte[]> getProducer(String topic) {
        if (forcedTopic) {
            return singleProducer;
        }

        if (topic2Producer.containsKey(topic)) {
            return topic2Producer.get(topic);
        } else {
            uploadSchema(topic);
            Producer p = createProducer(clientConfigurationData, producerConf, topic);
            topic2Producer.put(topic, p);
            return p;
        }
    }

    /*    */

    /**
     * To determine how to generate schema for pulsar producer
     * @return the schema that our producer use.
     *//*
    protected abstract Schema<T> buildSchema();*/
    protected Producer<byte[]> createProducer(
            ClientConfigurationData clientConf,
            Map<String, Object> producerConf,
            String topic
    ) {
        try {
            return CachedPulsarClient
                    .getOrCreate(clientConf)
                    .newProducer(Schema.AUTO_PRODUCE_BYTES(serializationSchema.getPulsarSchema()))
                    .topic(topic)
                    .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                    // maximizing the throughput
                    .batchingMaxBytes(5 * 1024 * 1024)
                    .loadConf(producerConf)
                    .create();
        } catch (PulsarClientException e) {
            log.error("Failed to create producer for topic {}", topic);
            throw new RuntimeException(e);
        }
    }

    public void producerFlush() throws Exception {
        if (singleProducer != null) {
            singleProducer.flush();
        } else {
            if (topic2Producer != null) {
                for (Producer<?> p : topic2Producer.values()) {
                    p.flush();
                }
            }
        }
        synchronized (pendingRecordsLock) {
            while (pendingRecords > 0) {
                try {
                    pendingRecordsLock.wait();
                } catch (InterruptedException e) {
                    // this can be interrupted when the Task has been cancelled.
                    // by throwing an exception, we ensure that this checkpoint doesn't get confirmed
                    throw new RuntimeException("Flushing got interrupted while checkpointing", e);
                }
            }
        }
    }

    protected void producerClose() throws Exception {
        producerFlush();
        if (admin != null) {
            admin.close();
        }
        if (singleProducer != null) {
            singleProducer.close();
        } else {
            if (topic2Producer != null) {
                for (Producer<?> p : topic2Producer.values()) {
                    p.close();
                }
                topic2Producer.clear();
            }
        }
    }

    protected void checkErroneous() throws Exception {
        Throwable e = failedWrite;
        if (e != null) {
            // prevent double throwing
            failedWrite = null;
            throw new Exception("Failed to send data to Pulsar: " + e.getMessage(), e);
        }
    }

    private void acknowledgeMessage() {
        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords--;
                if (pendingRecords == 0) {
                    pendingRecordsLock.notifyAll();
                }
            }
        }
    }
}
