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

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.BrokerPartition;
import org.apache.flink.connector.pulsar.source.MessageDeserializer;
import org.apache.flink.connector.pulsar.source.PartitionReader;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions.OffsetVerification;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer.CreationConfiguration;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.util.AsyncUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.apache.flink.util.ComponentClosingUtils.closeWithTimeout;

/**
 * A {@link SplitReader} implementation that reads records from Pulsar partitions.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 *
 * @param <T> the type of the record to be emitted from the Source.
 */
public class PulsarPartitionSplitReader<T> implements SplitReader<ParsedMessage<T>, PulsarPartitionSplit>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarPartitionSplitReader.class);

    private final PriorityQueue<PartitionReader> readerQueue = new PriorityQueue<>();
    private final SimpleCollector<T> collector = new SimpleCollector<>();
    private final ConsumerConfigurationData<byte[]> consumerConfigurationData;
    private final PulsarClient client;
    private final PulsarAdmin pulsarAdmin;
    private final MessageDeserializer<T> messageDeserializer;
    private final Duration maxFetchTime;
    private final int maxFetchRecords;
    private final long closeTimeout;
    private final OffsetVerification offsetVerification;
    private volatile boolean wakeup;
    private final ExecutorProvider listenerExecutor;

    public PulsarPartitionSplitReader(
            Configuration configuration,
            ConsumerConfigurationData<byte[]> consumerConfigurationData,
            PulsarClient client,
            PulsarAdmin pulsarAdmin,
            MessageDeserializer<T> messageDeserializer,
            ExecutorProvider listenerExecutor) {
        this.consumerConfigurationData = consumerConfigurationData;
        this.client = client;
        this.pulsarAdmin = pulsarAdmin;
        this.messageDeserializer = messageDeserializer;
        maxFetchTime = Duration.ofMillis(configuration.get(PulsarSourceOptions.MAX_FETCH_TIME));
        maxFetchRecords = configuration.get(PulsarSourceOptions.MAX_FETCH_RECORDS);
        closeTimeout = configuration.get(PulsarSourceOptions.CLOSE_TIMEOUT_MS);
        offsetVerification = configuration.get(PulsarSourceOptions.VERIFY_INITIAL_OFFSETS);
        this.listenerExecutor = listenerExecutor;
    }

    @Override
    public void close() {
        closeWithTimeout(
                "PulsarSourceEnumerator",
                (ThrowingRunnable<Exception>) () -> {
                    try (Closer closer = Closer.create()) {
                        readerQueue.forEach(closer::register);
                    }
                },
                closeTimeout);
    }

    @Override
    public RecordsWithSplitIds<ParsedMessage<T>> fetch() {
        wakeup = false;
        PulsarPartitionSplitRecords<ParsedMessage<T>> recordsBySplits = new PulsarPartitionSplitRecords<>();
        if (readerQueue.isEmpty()) {
            return recordsBySplits;
        }

        Deadline deadline = Deadline.fromNow(maxFetchTime);
        for (int numRecords = 0; numRecords < maxFetchRecords && !readerQueue.isEmpty() && deadline.hasTimeLeft() && !wakeup; numRecords++) {
            PartitionReader reader = readerQueue.poll();
            try {
                Iterator<Message<?>> messages = reader.nextBatch();
                if (messages.hasNext()) {
                    while (messages.hasNext()) {
                        Message<?> message = messages.next();

                        Collection<ParsedMessage<T>> recordsForSplit = recordsBySplits.recordsForSplit(reader.getSplit().splitId());
                        messageDeserializer.deserialize(message, collector);
                        collector.getRecords().forEach(r -> recordsForSplit.add(new ParsedMessage<>(
                                r,
                                message.getMessageId(),
                                message.getEventTime())));
                        collector.reset();
                    }
                }
                if (reader.isStopped()) {
                    LOG.debug(
                            "{} has reached stopping condition, current offset is {} @ timestamp {}",
                            reader.getSplit(),
                            reader.getLastMessage().getMessageId(),
                            reader.getLastMessage().getEventTime());
                    recordsBySplits.addFinishedSplit(reader.getSplit().splitId());
                    reader.close();
                } else {
                    readerQueue.add(reader);
                }
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Error while fetching from " + reader.getSplit());
            }
        }
        recordsBySplits.prepareForRead();
        return recordsBySplits;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PulsarPartitionSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(String.format(
                    "The SplitChange type of %s is not supported.", splitsChange.getClass()));
        }
        try {
            AsyncUtils.parallelAsync(splitsChange.splits(), this::createPartitionReaderAsync, (partition, reader) -> readerQueue.add(reader), PulsarClientException.class);
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot create reader", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Cannot create reader: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    public CompletableFuture<PartitionReader> createPartitionReaderAsync(PulsarPartitionSplit split) throws PulsarClientException {
        AbstractPartition abstractPartition = split.getPartition();
        CompletableFuture<PartitionReader> completableFuture = null;
        if (abstractPartition.getPartitionType() == AbstractPartition.PartitionType.Broker) {
            BrokerPartition partition = (BrokerPartition) abstractPartition;
            try {
                ConsumerConfigurationData<byte[]> conf = consumerConfigurationData.clone();
                CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
                if (partition.getTopicRange().getPulsarRange() != BrokerPartition.FULL_RANGE) {
                    conf.setKeySharedPolicy(KeySharedPolicy
                            .stickyHashRange()
                            .ranges(partition.getTopicRange().getPulsarRange()));
                    conf.setSubscriptionName(conf.getSubscriptionName() + partition.getTopicRange().getPulsarRange());
                }
                MessageId lastConsumedId = split.getLastConsumedId();
                StartOffsetInitializer startOffsetInitializer = lastConsumedId != null ?
                        StartOffsetInitializer.offset(lastConsumedId, false) :
                        split.getStartOffsetInitializer();
                // initialize offset on builder for absolute offsets
                CreationConfiguration creationConfiguration = new CreationConfiguration(conf);
                startOffsetInitializer.initializeBeforeCreation(partition, creationConfiguration);
                ConsumerImpl<byte[]> consumer = new ConsumerImpl<byte[]>(
                        (PulsarClientImpl) client,
                        partition.getTopic(),
                        creationConfiguration.getConsumerConfigurationData(),
                        listenerExecutor,
                        TopicName.getPartitionIndex(partition.getTopic()),
                        false,
                        subscribeFuture,
                        creationConfiguration.getInitialMessageId(),
                        creationConfiguration.getRollbackInS(),
                        Schema.BYTES,
                        null,
                        true) {
                };
                // initialize offset on reader for time-based seeking
                startOffsetInitializer.initializeAfterCreation(partition, consumer);
                split.getStopCondition().init(partition, consumer);

                if (offsetVerification != OffsetVerification.IGNORE) {
                    startOffsetInitializer.verifyOffset(
                            partition,
                            wrap(() -> Optional.ofNullable(pulsarAdmin.topics().getLastMessageId(partition.getTopic()))),
                            wrap(() -> pulsarAdmin.topics().peekMessages(partition.getTopic(), conf.getSubscriptionName(), 1).stream().findFirst()))
                            .ifPresent(error -> reportDataLoss(partition, error));
                }

                completableFuture = subscribeFuture.thenApply(c -> new PartitionReader(split, consumer, split.getStopCondition()));
            } catch (PulsarClientException.TopicDoesNotExistException e) {
                throw new IllegalStateException("Cannot subscribe to partition " + partition, e);
            } catch (PulsarClientException e) {
                throw new IllegalStateException("Cannot add split " + split, e);
            } catch (Exception e) {
                throw PulsarClientException.unwrap(e);
            }
        }
        //for now we just support broker type partition.
        return completableFuture;
    }

    private <T> Supplier<T> wrap(SupplierWithException<T, ?> supplierWithException) {
        return () -> {
            try {
                return supplierWithException.get();
            } catch (Throwable throwable) {
                ExceptionUtils.rethrow(throwable);
                return null;
            }
        };
    }

    private void reportDataLoss(AbstractPartition partition, String error) {
        String fullError = String.format("While initializing %s encountered the following error: %s.\n" +
                        "Possible reasons include data being already deleted because of wrong retention or wrong offsets.\n" +
                        "To change the behavior of the offset verification, please refer to the option \"%s\".",
                partition,
                error,
                PulsarSourceOptions.VERIFY_INITIAL_OFFSETS.key());
        if (offsetVerification == OffsetVerification.FAIL_ON_MISMATCH) {
            throw new IllegalStateException(fullError);
        }
        LOG.warn(fullError);
    }

    @Override
    public void wakeUp() {
        wakeup = true;
    }

    private static class PulsarPartitionSplitRecords<T> implements RecordsWithSplitIds<T> {
        private final Map<String, Collection<T>> recordsBySplits;
        private final Set<String> finishedSplits;
        private Iterator<Map.Entry<String, Collection<T>>> splitIterator;
        private String currentSplitId;
        private Iterator<T> recordIterator;

        private PulsarPartitionSplitRecords() {
            this.recordsBySplits = new HashMap<>();
            this.finishedSplits = new HashSet<>();
        }

        private Collection<T> recordsForSplit(String splitId) {
            return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
        }

        private void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
        }

        private void prepareForRead() {
            splitIterator = recordsBySplits.entrySet().iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                Map.Entry<String, Collection<T>> entry = splitIterator.next();
                currentSplitId = entry.getKey();
                recordIterator = entry.getValue().iterator();
                return currentSplitId;
            } else {
                currentSplitId = null;
                recordIterator = null;
                return null;
            }
        }

        @Override
        @Nullable
        public T nextRecordFromSplit() {
            Preconditions.checkNotNull(currentSplitId, "Make sure nextSplit() did not return null before " +
                    "iterate over the records split.");
            if (recordIterator.hasNext()) {
                return recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }

    private static class SimpleCollector<T> implements Collector<T> {
        private final List<T> records = new ArrayList<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void close() {

        }

        private List<T> getRecords() {
            return records;
        }

        private void reset() {
            records.clear();
        }
    }

}
