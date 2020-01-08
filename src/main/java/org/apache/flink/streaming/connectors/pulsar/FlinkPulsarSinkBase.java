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
package org.apache.flink.streaming.connectors.pulsar;

import avro.shaded.com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.util.SerializableObject;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

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

    protected final TopicKeyExtractor<T> topicKeyExtractor;

    protected transient volatile Throwable failedWrite;

    protected PulsarAdmin admin;

    protected transient BiConsumer<MessageId, Throwable> sendCallback;

    protected transient Producer<?> singleProducer;

    protected transient Map<String, Producer<?>> topic2Producer;

    public FlinkPulsarSinkBase(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            TopicKeyExtractor<T> topicKeyExtractor) {

        this.adminUrl = checkNotNull(adminUrl);

        if (defaultTopicName.isPresent()) {
            this.forcedTopic = true;
            this.defaultTopic = defaultTopicName.get();
            this.topicKeyExtractor = null;
        } else {
            this.forcedTopic = false;
            this.defaultTopic = null;
            ClosureCleaner.clean(
                topicKeyExtractor, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            this.topicKeyExtractor = checkNotNull(topicKeyExtractor);
        }

        this.properties = checkNotNull(properties);

        this.caseInsensitiveParams =
            SourceSinkUtils.toCaceInsensitiveParams(Maps.fromProperties(properties));

        this.producerConf =
            SourceSinkUtils.getProducerParams(caseInsensitiveParams);

        this.flushOnCheckpoint =
            SourceSinkUtils.flushOnCheckpoint(caseInsensitiveParams);

        this.failOnWrite =
            SourceSinkUtils.failOnWrite(caseInsensitiveParams);

        CachedPulsarClient.setCacheSize(SourceSinkUtils.getClientCacheSize(caseInsensitiveParams));
    }

    public FlinkPulsarSinkBase(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            TopicKeyExtractor<T> topicKeyExtractor) {
        this(adminUrl, defaultTopicName, new ClientConfigurationData(), properties, topicKeyExtractor);
        this.clientConfigurationData.setServiceUrl(serviceUrl);
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

    protected abstract Schema<?> getPulsarSchema();

    @Override
    public void open(Configuration parameters) throws Exception {
        if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
            log.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
            flushOnCheckpoint = false;
        }

        admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

        if (forcedTopic) {
            uploadSchema(defaultTopic);
            singleProducer = createProducer(clientConfigurationData, producerConf, defaultTopic, getPulsarSchema());
        } else {
            topic2Producer = new HashMap<>();
        }

        if (failOnWrite) {
            this.sendCallback = (t, u) -> {
                if (failedWrite == null && u == null) {
                    acknowledgeMessage();
                } else if (failedWrite == null && u != null) {
                    failedWrite = u;
                } else { // failedWrite != null
                    // do nothing and wait next checkForError to throw exception
                }
            };
        } else {
            this.sendCallback = (t, u) -> {
                if (failedWrite == null && u != null) {
                    log.error("Error while sending message to Pulsar: %s", ExceptionUtils.getFullStackTrace(u));
                }
                acknowledgeMessage();
            };
        }
    }

    private void uploadSchema(String topic) {
        SchemaUtils.uploadPulsarSchema(admin, topic, getPulsarSchema().getSchemaInfo());
    }

    @Override
    public void close() throws Exception {
        checkErroneous();
        producerClose();
        checkErroneous();
    }

    protected Producer<T> getProducer(String topic) {
        if (singleProducer != null) {
            return (Producer<T>) singleProducer;
        }

        if (topic2Producer.containsKey(topic)) {
            return (Producer<T>) topic2Producer.get(topic);
        } else {
            uploadSchema(topic);
            val p = createProducer(clientConfigurationData, producerConf, topic, getPulsarSchema());
            topic2Producer.put(topic, p);
            return (Producer<T>) p;
        }
    }

    protected Producer<?> createProducer(
            ClientConfigurationData clientConf,
            Map<String, Object> producerConf,
            String topic,
            Schema<?> schema) {

        try {
            return CachedPulsarClient
                .getOrCreate(clientConf)
                .newProducer(schema)
                .topic(topic)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                // maximizing the throughput
                .batchingMaxMessages(5 * 1024 * 1024)
                .loadConf(producerConf)
                .create();
        } catch (PulsarClientException e) {
            log.error("Failed to create producer for topic %s", topic);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.error("Failed to getOrCreate a PulsarClient");
            throw new RuntimeException(e);
        }
    }

    public void producerFlush() throws Exception {
        if (singleProducer != null) {
            singleProducer.flush();
        } else {
            for (Producer<?> p : topic2Producer.values()) {
                p.flush();
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
        admin.close();
        if (singleProducer != null) {
            singleProducer.close();
        } else {
            for (Producer<?> p : topic2Producer.values()) {
                p.close();
            }
        }
        topic2Producer.clear();
    }

    protected void checkErroneous() throws Exception {
        Throwable e = failedWrite;
        if (e != null) {
            // prevent double throwing
            failedWrite = null;
            throw new Exception("Failed to send data to Kafka: " + e.getMessage(), e);
        }
    }

    private void acknowledgeMessage() {
        if (flushOnCheckpoint) {
            synchronized(pendingRecordsLock) {
                pendingRecords--;
                if (pendingRecords == 0) {
                    pendingRecordsLock.notifyAll();
                }
            }
        }
    }
}
