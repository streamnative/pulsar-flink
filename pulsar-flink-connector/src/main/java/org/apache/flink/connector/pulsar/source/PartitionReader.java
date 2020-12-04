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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;

/**
 * Using to reade data form partition.
 */
//TODO make this class abstract to implements streamPartitionReader and bkPartitionReader、tsPartitionReader
// to read from broker and bookie or tiredStorage.
public class PartitionReader implements Comparable<PartitionReader>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionReader.class);

    private static final long MAX_BACKOFF = 1L << 30;
    private final PulsarPartitionSplit split;
    private final ConsumerImpl<byte[]> consumer;
    private final StopCondition stopCondition;
    @Nullable
    private Message lastMessage;
    private long backOff = 1;
    private boolean stopped;

    public PartitionReader(PulsarPartitionSplit split, ConsumerImpl<byte[]> consumer, StopCondition stopCondition) {
        this.split = split;
        this.consumer = consumer;
        this.stopCondition = stopCondition;
    }

    public PulsarPartitionSplit getSplit() {
        return split;
    }

    @Nullable
    public Message getLastMessage() {
        return lastMessage;
    }

    public void setLastMessage(@Nullable Message lastMessage) {
        this.lastMessage = lastMessage;
    }

    public Iterator<Message<?>> nextBatch() throws PulsarClientException {
        if (consumer.hasMessageAvailable()) {
            Messages<byte[]> messages = consumer.batchReceive();
            Iterator<Message<byte[]>> messageIterator = messages.iterator();
            if (messageIterator.hasNext()) {
                backOff = 1;
                return new Iterator<Message<?>>() {
                    @Nullable
                    Message<byte[]> next = initNext();

                    @Override
                    public boolean hasNext() {
                        return next != null;
                    }

                    @Override
                    public Message<?> next() {
                        lastMessage = next;
                        next = initNext();
                        return lastMessage;
                    }

                    @Nullable
                    private Message<byte[]> initNext() {
                        if (!messageIterator.hasNext()) {
                            return null;
                        }
                        Message<byte[]> next = messageIterator.next();
                        switch (stopCondition.shouldStop(split.getPartition(), next)) {
                            case STOP_BEFORE:
                                stopped = true;
                                return null;
                            case STOP_AFTER:
                                stopped = true;
                                return next;
                            default:
                                return next;
                        }
                    }
                };
            }
        }

        if (backOff < MAX_BACKOFF) {
            backOff <<= 1;
        }
        return Collections.emptyIterator();
    }

    public boolean isStopped() {
        return stopped;
    }

    @Override
    public int compareTo(PartitionReader o) {
        return Long.compare(getOrder(), o.getOrder());
    }

    private long getOrder() {
        return lastMessage == null ? backOff : (lastMessage.getEventTime() + backOff);
    }

    public void close() {
        consumer.closeAsync().whenComplete((dummy, e) -> {
            if (e != null) {
                LOG.warn("Error while closing reader for " + split, e);
            }
        });
    }
}
