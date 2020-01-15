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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ReaderThread<T> extends Thread {

    protected final PulsarFetcher owner;
    protected final PulsarTopicState state;
    protected final ClientConfigurationData clientConf;
    protected final Map<String, Object> readerConf;
    protected final int pollTimeoutMs;
    protected final ExceptionProxy exceptionProxy;
    protected final String topic;
    protected final MessageId startMessageId;

    protected volatile boolean running = true;

    private final DeserializationSchema<T> deserializer;

    protected volatile Reader<?> reader = null;

    public ReaderThread(
            PulsarFetcher owner,
            PulsarTopicState state,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            DeserializationSchema<T> deserializer,
            int pollTimeoutMs,
            ExceptionProxy exceptionProxy) {
        this.owner = owner;
        this.state = state;
        this.clientConf = clientConf;
        this.readerConf = readerConf;
        this.deserializer = deserializer;
        this.pollTimeoutMs = pollTimeoutMs;
        this.exceptionProxy = exceptionProxy;

        this.topic = state.getTopic();
        this.startMessageId = state.getOffset();
    }

    @Override
    public void run() {
        log.info(String.format("Starting to fetch from %s at %s", topic, startMessageId));

        try {
            createActualReader();

            skipFirstMessageIfNeeded();

            log.info(String.format("Starting to read %s with reader thread %s", topic, getName()));

            while (running) {
                Message message = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS);
                if (message != null) {
                    emitRecord(message);
                }
            }
        } catch (Throwable e) {
            exceptionProxy.reportError(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Throwable e) {
                    log.error("Error while closing Pulsar reader " + e.toString());
                }
            }
        }
    }

    protected void createActualReader() throws org.apache.pulsar.client.api.PulsarClientException, ExecutionException {
        reader = CachedPulsarClient
                .getOrCreate(clientConf)
                .newReader()
                .topic(topic)
                .startMessageId(startMessageId)
                .startMessageIdInclusive()
                .loadConf(readerConf)
                .create();
    }

    protected void skipFirstMessageIfNeeded() throws org.apache.pulsar.client.api.PulsarClientException {
        Message<?> currentMessage;
        MessageId currentId;
        if (startMessageId != MessageId.earliest && startMessageId != MessageId.latest) {
            currentMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS);
            if (currentMessage == null) {
                reportDataLoss(String.format("Cannot read data at offset %s from topic: %s",
                        startMessageId.toString(),
                        topic));
            } else {
                currentId = currentMessage.getMessageId();
                if (!messageIdRoughEquals(currentId, startMessageId)) {
                    reportDataLoss(
                            String.format(
                                    "Potential Data Loss in reading %s: intended to start at %s, actually we get %s",
                                    topic, startMessageId.toString(), currentId.toString()));
                }

                if (startMessageId instanceof BatchMessageIdImpl && currentId instanceof BatchMessageIdImpl) {
                    // we seek using a batch message id, we can read next directly later
                } else if (startMessageId instanceof MessageIdImpl && currentId instanceof BatchMessageIdImpl) {
                    // we seek using a message id, this is supposed to be read by previous task since it's
                    // inclusive for the checkpoint, so we skip this batch
                    BatchMessageIdImpl cbmid = (BatchMessageIdImpl) currentId;

                    MessageIdImpl newStart =
                            new MessageIdImpl(cbmid.getLedgerId(), cbmid.getEntryId() + 1, cbmid.getPartitionIndex());
                    reader.seek(newStart);
                } else if (startMessageId instanceof MessageIdImpl && currentId instanceof MessageIdImpl) {
                    // current entry is a non-batch entry, we can read next directly later
                }
            }
        }
    }

    protected void emitRecord(Message<?> message) throws IOException {
        MessageId messageId = message.getMessageId();
        T record = deserializer.deserialize(message.getData());
        owner.emitRecord(record, state, messageId);
    }

    public void cancel() throws IOException {
        this.running = false;

        if (reader != null) {
            reader.close();
        }

        this.interrupt();
    }

    public boolean isRunning() {
        return running;
    }

    private void reportDataLoss(String message) {
        running = false;
        exceptionProxy.reportError(
                new IllegalStateException(message + PulsarOptions.INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE));
    }

    // used to check whether starting position and current message we got actually are equal
    // we neglect the potential batchIdx deliberately while seeking to MessageIdImpl for batch entry
    public static boolean messageIdRoughEquals(MessageId l, MessageId r) {
        if (l == null || r == null) {
            return false;
        }

        if (l instanceof BatchMessageIdImpl && r instanceof BatchMessageIdImpl) {
            return l.equals(r);
        } else if (l instanceof MessageIdImpl && r instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl rb = (BatchMessageIdImpl) r;
            return l.equals(new MessageIdImpl(rb.getLedgerId(), rb.getEntryId(), rb.getPartitionIndex()));
        } else if (r instanceof MessageIdImpl && l instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl lb = (BatchMessageIdImpl) l;
            return r.equals(new MessageIdImpl(lb.getLedgerId(), lb.getEntryId(), lb.getPartitionIndex()));
        } else if (l instanceof MessageIdImpl && r instanceof MessageIdImpl) {
            return l.equals(r);
        } else {
            throw new IllegalStateException(
                    String.format("comparing messageIds of type %s, %s", l.getClass().toString(), r.getClass().toString()));
        }
    }
}
