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

package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.common.ConnectorConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.collect.Lists;
import org.apache.pulsar.shade.org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.pulsar.shade.org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.pulsar.shade.org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.shade.org.jctools.queues.MessagePassingQueue;
import org.apache.pulsar.shade.org.jctools.queues.SpscArrayQueue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Segment reader for a input split.
 * @param <T> the type of emit data.
 */
@Slf4j
public abstract class InputSplitReader<T> {

    private ConnectorConfig connectorConfig;

    private int partitionId;

    private List<InputLedger> ledgersToRead;

    private CachedClients cachedClients;
    private BookKeeper bkClient;
    private ManagedLedgerConfig mlConfig;
    private LedgerOffloader offloader;

    private Executor executor;

    private SpscArrayQueue<RawMessage> messageQueue;
    private SpscArrayQueue<Entry> entryQueue;

    private Thread deserializerThread;
    private RawMessage currentMessage;

    private AtomicLong outstandingLedgerReads = new AtomicLong(0);
    private int ledgerSize;
    private int currentLedgerIdx = 0;

    private long partitionSize;
    private long entriesProcessed = 0;

    private Map<Long, TopicName> ledger2Topic = new ConcurrentHashMap<>();

    public InputSplitReader(ConnectorConfig connectorConfig, int partitionId, List<InputLedger> ledgersToRead)
            throws Exception {
        this.connectorConfig = connectorConfig;
        this.partitionId = partitionId;
        this.ledgersToRead = ledgersToRead;

        this.cachedClients = CachedClients.getInstance(connectorConfig);
        this.bkClient = cachedClients.getManagedLedgerFactory().getBookKeeper();
        this.mlConfig = cachedClients.getManagedLedgerConfig();
        this.offloader = mlConfig.getLedgerOffloader();

        this.executor = Executors.newSingleThreadExecutor();

        this.messageQueue = new SpscArrayQueue<>(connectorConfig.getMaxSplitMessageQueueSize());
        this.entryQueue = new SpscArrayQueue<>(connectorConfig.getMaxSplitEntryQueueSize());

        this.ledgerSize = ledgersToRead.size();

        this.partitionSize = ledgersToRead.stream().mapToLong(InputLedger::ledgerSize).sum();
    }

    public boolean next() throws IOException {
        if (deserializerThread == null) {
            deserializerThread = new DeserializeEntries();
            deserializerThread.start();

            getEntries(ledgersToRead.get(currentLedgerIdx));
            currentLedgerIdx++;
        }

        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }

        while (true) {
            if (messageQueue.isEmpty() && entriesProcessed >= partitionSize) {
                return false;
            }

            if (currentLedgerIdx < ledgerSize && outstandingLedgerReads.get() == 0) {
                getEntries(ledgersToRead.get(currentLedgerIdx));
                currentLedgerIdx++;
            }

            currentMessage = messageQueue.poll();
            if (currentMessage != null) {
                return true;
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public abstract T deserialize(RawMessage currentMessage) throws IOException;

    public T get() throws IOException {
        return deserialize(currentMessage);
    }

    public void close() throws Exception {
        if (currentMessage != null) {
            currentMessage.release();
        }

        if (messageQueue != null) {
            messageQueue.drain(RawMessage::release);
        }

        if (entryQueue != null) {
            entryQueue.drain(Entry::release);
        }

        if (deserializerThread != null) {
            deserializerThread.interrupt();
        }
    }

    CompletableFuture<Object> getEntries(InputLedger info) {
        outstandingLedgerReads.incrementAndGet();

        return getLedgerHandle(info).thenComposeAsync(readHandle -> {
            try (LedgerEntries entries = readHandle.read(info.getStartEntryId(), info.getEndEntryId())) {

                entryQueue.fill(new MessagePassingQueue.Supplier<Entry>() {
                    private int i = 0;

                    @Override
                    public Entry get() {
                        EntryImpl impl = EntryImpl.create(entries.getEntry(i));
                        i++;
                        return impl;
                    }
                }, Lists.newArrayList(entries.iterator()).size());

            } catch (Exception e) {
                throw new CompletionException(e);
            }
            return null;
        }, executor).whenComplete((t, throwable) -> {
            if (throwable != null) {
                log.error(String.format("Get entry failed due to %s", throwable.getMessage()), throwable);
            } else {
                log.info(String.format("Finished extracting entries for ledger %s", info.toString()));
                outstandingLedgerReads.decrementAndGet();
            }
        });
    }

    CompletableFuture<ReadHandle> getLedgerHandle(InputLedger ledger) {
        ledger2Topic.put(ledger.getLedgerId(), TopicName.get(ledger.getTopic()));
        if (ledger.getUuid() != null) {
            return offloader.readOffloaded(ledger.getLedgerId(), ledger.getUuid(), ledger.getOffloaderDrvierMeta());
        } else {
            return bkClient.newOpenLedgerOp()
                    .withRecovery(false)
                    .withLedgerId(ledger.getLedgerId())
                    .withDigestType(mlConfig.getDigestType())
                    .withPassword(mlConfig.getPassword())
                    .execute();
        }
    }

    class DeserializeEntries extends Thread {

        protected boolean isRunning = false;

        DeserializeEntries() {
            super("derserialize-thread-split-" + partitionId);
        }

        @Override
        public void interrupt() {
            isRunning = false;
        }

        @Override
        public void run() {
            isRunning = true;
            while (isRunning) {

                int read = entryQueue.drain(entry -> {
                    TopicName tp = ledger2Topic.getOrDefault(entry.getLedgerId(), TopicName.get("DUMMY"));
                    try {
                        MessageParser.parseMessage(tp, entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer(),
                                (message) -> {
                                    try {
                                        // enqueue deserialize message from this entry
                                        while (!messageQueue.offer(message)) {
                                            Thread.sleep(1);
                                        }

                                    } catch (InterruptedException e) {
                                        //no-op
                                    }
                                }, connectorConfig.getMaxMessageSize());
                    } catch (IOException e) {
                        log.error(String.format("Failed to parse message from pulsar topic %s", tp.toString()), e);
                    } finally {
                        entriesProcessed++;
                        entry.release();
                    }
                });

                if (read <= 0) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }

}
