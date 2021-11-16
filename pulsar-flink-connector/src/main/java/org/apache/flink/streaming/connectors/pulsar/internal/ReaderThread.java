/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.streaming.connectors.pulsar.serialization.PulsarDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.util.MessageIdUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Actual working thread that read a specific Pulsar topic.
 *
 * @param <T> the record type that read from each Pulsar message.
 */
@Slf4j
public class ReaderThread<T> extends Thread {

    protected final PulsarFetcher<T> owner;
    protected final PulsarTopicState<T> state;
    protected final ClientConfigurationData clientConf;
    protected final Map<String, Object> readerConf;
    protected final int pollTimeoutMs;
    protected final ExceptionProxy exceptionProxy;
    protected final TopicRange topicRange;
    protected MessageId startMessageId;
    protected boolean excludeMessageId = false;
    private boolean failOnDataLoss = true;
    private boolean useEarliestWhenDataLoss = false;

    protected volatile boolean running = true;
    protected volatile boolean closed = false;

    protected final PulsarDeserializationSchema<T> deserializer;

    protected volatile Reader<T> reader = null;

    private final long startConsumeTimestamp;

    public ReaderThread(
            PulsarFetcher<T> owner,
            PulsarTopicState state,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            PulsarDeserializationSchema<T> deserializer,
            int pollTimeoutMs,
            ExceptionProxy exceptionProxy,
            long startConsumeTimestamp) {
        this.owner = owner;
        this.state = state;
        this.clientConf = clientConf;
        this.readerConf = readerConf;
        this.deserializer = deserializer;
        this.pollTimeoutMs = pollTimeoutMs;
        this.exceptionProxy = exceptionProxy;

        this.topicRange = state.getTopicRange();
        this.startMessageId = state.getOffset();
        this.startConsumeTimestamp = startConsumeTimestamp;
    }

    public ReaderThread(
            PulsarFetcher<T> owner,
            PulsarTopicState state,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            PulsarDeserializationSchema<T> deserializer,
            int pollTimeoutMs,
            ExceptionProxy exceptionProxy,
            boolean failOnDataLoss,
            boolean useEarliestWhenDataLoss,
            boolean excludeMessageId,
            long startConsumeTimestamp) {
        this(
                owner,
                state,
                clientConf,
                readerConf,
                deserializer,
                pollTimeoutMs,
                exceptionProxy,
                startConsumeTimestamp);
        this.failOnDataLoss = failOnDataLoss;
        this.useEarliestWhenDataLoss = useEarliestWhenDataLoss;
        this.excludeMessageId = excludeMessageId;
    }

    @Override
    public void run() {
        log.info(
                "Starting to fetch from {} at {}, failOnDataLoss {}",
                topicRange,
                startMessageId,
                failOnDataLoss);

        try {
            handleTooLargeCursor();
            createActualReader();
            log.info("Starting to read {} with reader thread {}", topicRange, getName());
            if (startConsumeTimestamp >= 0) {
                log.info("Reader seek to timestamp:{}", startConsumeTimestamp);
                reader.seek(startConsumeTimestamp);
            }

            while (running) {
                Message<T> message = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS);
                if (message != null) {
                    emitRecord(message);
                }
            }
        } catch (Throwable e) {
            exceptionProxy.reportError(e);
        } finally {
            if (reader != null) {
                try {
                    close();
                } catch (Throwable e) {
                    log.error("Error while closing Pulsar reader " + e.toString());
                }
            }
        }
    }

    protected void createActualReader() throws PulsarClientException {
        ReaderBuilder<T> readerBuilder =
                CachedPulsarClient.getOrCreate(clientConf)
                        .newReader(deserializer.getSchema())
                        .topic(topicRange.getTopic())
                        .startMessageId(startMessageId)
                        .loadConf(readerConf);
        log.info(
                "Create a reader at topic {} starting from message {} (inclusive) : config = {}",
                topicRange,
                startMessageId,
                readerConf);
        if (!excludeMessageId) {
            readerBuilder.startMessageIdInclusive();
        }
        if (!topicRange.isFullRange()) {
            readerBuilder.keyHashRange(topicRange.getPulsarRange());
        }

        reader = readerBuilder.create();
    }

    protected void handleTooLargeCursor() {
        if (startMessageId.equals(MessageId.earliest)
                || startMessageId.equals(MessageId.latest)
                || ((MessageIdImpl) startMessageId).getEntryId() == -1) {
            return;
        }

        MessageId lastMessageId =
                this.owner.getMetaDataReader().getLastMessageId(topicRange.getTopic());
        if (MessageIdUtils.prev(startMessageId).compareTo(lastMessageId) <= 0) {
            return;
        }
        // Because the topic has processed all the messages,
        // this will make the messageId to be read greater than the lastMessageId.
        // startMessageId is bigger than lastMessageId + 1
        if (this.failOnDataLoss) {
            log.error(
                    "the start message id is beyond the last commit message id, with topic:{}",
                    this.topicRange);
            throw new IllegalStateException("start message id beyond the last commit");
        } else if (useEarliestWhenDataLoss) {
            log.info("reset message to earliest");
            startMessageId = MessageId.earliest;
        } else {
            log.info("reset message to valid offset {}", lastMessageId);
            startMessageId = lastMessageId;
        }
    }

    protected void emitRecord(Message<T> message) throws IOException {
        MessageId messageId = message.getMessageId();
        final T record = deserializer.deserialize(message);
        if (deserializer.isEndOfStream(record)) {
            running = false;
            return;
        }
        owner.emitRecordsWithTimestamps(record, state, messageId, message.getEventTime());
    }

    public void cancel() throws IOException {
        this.running = false;

        if (reader != null) {
            try {
                close();
            } catch (IOException e) {
                log.error("failed to close reader. ", e);
            }
        }

        this.interrupt();
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        reader.close();
        log.info("Reader closed");
    }

    public boolean isRunning() {
        return running;
    }

    private void reportDataLoss(String message) {
        running = false;
        exceptionProxy.reportError(
                new IllegalStateException(
                        message + PulsarOptions.INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE));
    }

    /**
     * used to check whether starting position and current message we got actually are equal we
     * neglect the potential batchIdx deliberately while seeking to MessageIdImpl for batch entry.
     */
    public static boolean messageIdRoughEquals(MessageId l, MessageId r) {
        if (l == null || r == null) {
            return false;
        }

        if (l instanceof BatchMessageIdImpl && r instanceof BatchMessageIdImpl) {
            return l.equals(r);
        } else if (l instanceof MessageIdImpl && r instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl rb = (BatchMessageIdImpl) r;
            return l.equals(
                    new MessageIdImpl(rb.getLedgerId(), rb.getEntryId(), rb.getPartitionIndex()));
        } else if (r instanceof MessageIdImpl && l instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl lb = (BatchMessageIdImpl) l;
            return r.equals(
                    new MessageIdImpl(lb.getLedgerId(), lb.getEntryId(), lb.getPartitionIndex()));
        } else if (l instanceof MessageIdImpl && r instanceof MessageIdImpl) {
            return l.equals(r);
        } else {
            throw new IllegalStateException(
                    String.format(
                            "comparing messageIds of type %s, %s",
                            l.getClass().toString(), r.getClass().toString()));
        }
    }
}
