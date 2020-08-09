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

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implements the logic around emitting records and tracking offsets,
 * as well as around the optional timestamp assignment and watermark generation.
 *
 * @param <T> The type of elements deserialized from Pulsar messages, and emitted into
 *           the Flink data stream.
 */
@Slf4j
public class PulsarFetcher<T> {

    private static final int NO_TIMESTAMPS_WATERMARKS = 0;
    private static final int PERIODIC_WATERMARKS = 1;
    private static final int PUNCTUATED_WATERMARKS = 2;

    // ------------------------------------------------------------------------

    /** The source context to emit records and watermarks to. */
    protected final SourceFunction.SourceContext<T> sourceContext;

    protected final Map<String, MessageId> seedTopicsWithInitialOffsets;

    /** The lock that guarantees that record emission and state updates are atomic,
     * from the view of taking a checkpoint. */
    private final Object checkpointLock;

    /** All partitions (and their state) that this fetcher is subscribed to. */
    protected final List<PulsarTopicState> subscribedPartitionStates;

    /**
     * Queue of partitions that are not yet assigned to any reader thread for consuming.
     *
     * <p>All partitions added to this queue are guaranteed to have been added
     * to {@link #subscribedPartitionStates} already.
     */
    protected final ClosableBlockingQueue<PulsarTopicState> unassignedPartitionsQueue;

    /** The mode describing whether the fetcher also generates timestamps and watermarks. */
    private final int timestampWatermarkMode;

    /**
     * Optional timestamp extractor / watermark generator that will be run per Pulsar partition,
     * to exploit per-partition timestamp characteristics.
     * The assigner is kept in serialized form, to deserialize it into multiple copies.
     */
    private final SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic;

    /**
     * Optional timestamp extractor / watermark generator that will be run per Pulsar partition,
     * to exploit per-partition timestamp characteristics.
     * The assigner is kept in serialized form, to deserialize it into multiple copies.
     */
    private final SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated;

    /** User class loader used to deserialize watermark assigners. */
    private final ClassLoader userCodeClassLoader;

    private final StreamingRuntimeContext runtimeContext;

    protected final ClientConfigurationData clientConf;

    protected final Map<String, Object> readerConf;

    protected final boolean failOnDataLoss;

    protected final PulsarDeserializationSchema<T> deserializer;

    protected final int pollTimeoutMs;

    private final int commitMaxRetries;

    protected final PulsarMetadataReader metadataReader;

    /** Only relevant for punctuated watermarks: The current cross partition watermark. */
    private volatile long maxWatermarkSoFar = Long.MIN_VALUE;

    /** Flag to mark the main work loop as alive. */
    private volatile boolean running = true;

    /** The threads that runs the actual reading and hand the records to this fetcher. */
    private Map<String, ReaderThread> topicToThread;

    public PulsarFetcher(
            SourceContext<T> sourceContext,
            Map<String, MessageId> seedTopicsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            StreamingRuntimeContext runtimeContext,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            int pollTimeoutMs,
            PulsarDeserializationSchema<T> deserializer,
            PulsarMetadataReader metadataReader) throws Exception {
        this(
                sourceContext,
                seedTopicsWithInitialOffsets,
                watermarksPeriodic,
                watermarksPunctuated,
                processingTimeProvider,
                autoWatermarkInterval,
                userCodeClassLoader,
                runtimeContext,
                clientConf,
                readerConf,
                pollTimeoutMs,
                3, // commit retries before fail
                deserializer,
                metadataReader);
    }

    public PulsarFetcher(
            SourceContext<T> sourceContext,
            Map<String, MessageId> seedTopicsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            StreamingRuntimeContext runtimeContext,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            int pollTimeoutMs,
            int commitMaxRetries,
            PulsarDeserializationSchema<T> deserializer,
            PulsarMetadataReader metadataReader) throws Exception {

        this.sourceContext = sourceContext;
        this.seedTopicsWithInitialOffsets = seedTopicsWithInitialOffsets;
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.userCodeClassLoader = userCodeClassLoader;
        this.runtimeContext = runtimeContext;
        this.clientConf = clientConf;
        this.readerConf = readerConf;
        this.pollTimeoutMs = pollTimeoutMs;
        this.commitMaxRetries = commitMaxRetries;
        this.deserializer = deserializer;
        this.metadataReader = metadataReader;

        // figure out what we watermark mode we will be using
        this.watermarksPeriodic = watermarksPeriodic;
        this.watermarksPunctuated = watermarksPunctuated;

        if (watermarksPeriodic == null) {
            if (watermarksPunctuated == null) {
                // simple case, no watermarks involved
                timestampWatermarkMode = NO_TIMESTAMPS_WATERMARKS;
            } else {
                timestampWatermarkMode = PUNCTUATED_WATERMARKS;
            }
        } else {
            if (watermarksPunctuated == null) {
                timestampWatermarkMode = PERIODIC_WATERMARKS;
            } else {
                throw new IllegalArgumentException("Cannot have both periodic and punctuated watermarks");
            }
        }

        this.unassignedPartitionsQueue = new ClosableBlockingQueue<>();

        // initialize subscribed partition states with seed partitions
        this.subscribedPartitionStates = createPartitionStateHolders(
                seedTopicsWithInitialOffsets,
                timestampWatermarkMode,
                watermarksPeriodic,
                watermarksPunctuated,
                userCodeClassLoader);

        // check that all seed partition states have a defined offset
        for (PulsarTopicState state : subscribedPartitionStates) {
            if (!state.isOffsetDefined()) {
                throw new IllegalArgumentException("The fetcher was assigned seed partitions with undefined initial offsets.");
            }
        }

        // all seed partitions are not assigned yet, so should be added to the unassigned partitions queue
        for (PulsarTopicState state : subscribedPartitionStates) {
            unassignedPartitionsQueue.add(state);
        }

        // if we have periodic watermarks, kick off the interval scheduler
        if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
            @SuppressWarnings("unchecked")
            PeriodicWatermarkEmitter periodicEmitter = new PeriodicWatermarkEmitter(
                    subscribedPartitionStates,
                    sourceContext,
                    processingTimeProvider,
                    autoWatermarkInterval);

            periodicEmitter.start();
        }

        // get failOnDataLoss from reader conf
        if (readerConf != null && readerConf.containsKey(PulsarOptions.FAIL_ON_DATA_LOSS_OPTION_KEY)) {
            String failOnDataLossVal = readerConf.getOrDefault(PulsarOptions.FAIL_ON_DATA_LOSS_OPTION_KEY, "true").toString();
            failOnDataLoss = Boolean.parseBoolean(failOnDataLossVal);
        } else {
            failOnDataLoss = true;
        }
    }

    public void runFetchLoop() throws Exception {
        topicToThread = new HashMap<>();
        ExceptionProxy exceptionProxy = new ExceptionProxy(Thread.currentThread());

        try {

            while (running) {
                // re-throw any exception from the concurrent fetcher threads
                exceptionProxy.checkAndThrowException();

                // wait for max 5 seconds trying to get partitions to assign
                // if threads shut down, this poll returns earlier, because the threads inject the
                // special marker into the queue
                List<PulsarTopicState> topicsToAssign = unassignedPartitionsQueue.getBatchBlocking(5000);
                // if there are more markers, remove them all
                topicsToAssign.removeIf(s -> s.equals(PoisonState.INSTANCE));

                if (!topicsToAssign.isEmpty()) {

                    if (!running) {
                        throw BreakingException.INSTANCE;
                    }

                    topicToThread.putAll(
                            createAndStartReaderThread(topicsToAssign, exceptionProxy));

                } else {
                    // there were no partitions to assign. Check if any consumer threads shut down.
                    // we get into this section of the code, if either the poll timed out, or the
                    // blocking poll was woken up by the marker element

                    topicToThread.values().removeIf(t -> !t.isRunning());

                }

                if (topicToThread.size() == 0 && unassignedPartitionsQueue.isEmpty()) {
                    PulsarTopicState topicForBlocking = unassignedPartitionsQueue.getElementBlocking();
                    if (topicForBlocking.equals(PoisonState.INSTANCE)) {
                        throw BreakingException.INSTANCE;
                    }
                    topicToThread.putAll(
                            createAndStartReaderThread(ImmutableList.of(topicForBlocking), exceptionProxy));
                }
            }

        } catch (BreakingException b) {
            // do nothing
        } catch (InterruptedException e) {
            // this may be thrown because an exception on one of the concurrent fetcher threads
            // woke this thread up. make sure we throw the root exception instead in that case
            exceptionProxy.checkAndThrowException();

            // no other root exception, throw the interrupted exception
            throw e;
        } finally {
            running = false;

            // clear the interruption flag
            // this allows the joining on reader threads (on best effort) to happen in
            // case the initial interrupt already
            Thread.interrupted();

            // make sure that in any case (completion, abort, error), all spawned threads are stopped
            try {
                int runningThreads = 0;
                do { // check whether threads are alive and cancel them
                    runningThreads = 0;

                    // remove thread which is not alive
                    topicToThread.values().removeIf(t -> !t.isAlive());

                    for (ReaderThread t : topicToThread.values()) {
                        t.cancel();
                        runningThreads++;
                    }

                    if (runningThreads > 0) {
                        for (ReaderThread t : topicToThread.values()) {
                            t.join(500 / runningThreads + 1);
                        }

                    }

                } while (runningThreads > 0);

            } catch (InterruptedException ignored) {
                // waiting for the thread shutdown apparently got interrupted
                // restore interrupted state and continue
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                // we catch all here to preserve the original exception
                log.error("Exception while shutting down reader threads", t);
            }
        }
    }

    protected void emitRecord(
            T record,
            PulsarTopicState topicState,
            MessageId offset) {

        if (record != null) {
            switch (timestampWatermarkMode) {
                case NO_TIMESTAMPS_WATERMARKS:
                    synchronized (checkpointLock) {
                        sourceContext.collect(record);
                        topicState.setOffset(offset);
                    }
                    return;
                case PERIODIC_WATERMARKS:
                    emitRecordWithTimestampAndPeriodicWatermark(record, topicState, offset, Long.MIN_VALUE);
                    return;
                case PUNCTUATED_WATERMARKS:
                    emitRecordWithTimestampAndPunctuatedWatermark(record, topicState, offset, Long.MIN_VALUE);
                    return;
            }
        } else {
            synchronized (checkpointLock) {
                topicState.setOffset(offset);
            }
        }
    }

    private void emitRecordWithTimestampAndPeriodicWatermark(
            T record, PulsarTopicState topicState, MessageId offset, long eventTimestamp) {

        PulsarTopicStateWithPeriodicWatermarks<T> periodicState = (PulsarTopicStateWithPeriodicWatermarks<T>) topicState;

        long timestamp = 0;

        synchronized (periodicState) {
            timestamp = periodicState.getTimestampForRecord(record, eventTimestamp);
        }

        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, timestamp);
            topicState.setOffset(offset);
        }
    }

    private void emitRecordWithTimestampAndPunctuatedWatermark(
            T record, PulsarTopicState topicState, MessageId offset, long eventTimestamp) {

        PulsarTopicStateWithPunctuatedWatermarks<T> punctuatedState = (PulsarTopicStateWithPunctuatedWatermarks<T>) topicState;
        long timestamp = punctuatedState.getTimestampForRecord(record, eventTimestamp);
        Watermark newWM = punctuatedState.checkAndGetNewWatermark(record, timestamp);

        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, timestamp);
            topicState.setOffset(offset);
        }

        if (newWM != null) {
            updateMinPunctuatedWatermark(newWM);
        }
    }

    private void updateMinPunctuatedWatermark(Watermark nextWatermark) {
        if (nextWatermark.getTimestamp() > maxWatermarkSoFar) {
            long newMin = Long.MAX_VALUE;
            for (PulsarTopicState state : subscribedPartitionStates) {
                PulsarTopicStateWithPunctuatedWatermarks<T> puncState = (PulsarTopicStateWithPunctuatedWatermarks<T>) state;
                newMin = Math.min(newMin, puncState.getCurrentPartitionWatermark());
            }

            // double-check locking pattern
            if (newMin > maxWatermarkSoFar) {
                synchronized (checkpointLock) {
                    if (newMin > maxWatermarkSoFar) {
                        maxWatermarkSoFar = newMin;
                        sourceContext.emitWatermark(new Watermark(newMin));
                    }
                }
            }
        }
    }

    public void cancel() throws Exception {
        // single the main thread to exit
        running = false;

        Set<String> topics = subscribedPartitionStates.stream()
                .map(PulsarTopicState::getTopic).collect(Collectors.toSet());

        metadataReader.removeCursor(topics);
        metadataReader.close();

        // make sure the main thread wakes up soon
        unassignedPartitionsQueue.addIfOpen(PoisonState.INSTANCE);
    }

    public void commitOffsetToPulsar(
            Map<String, MessageId> offset,
            PulsarCommitCallback offsetCommitCallback) throws InterruptedException {

        doCommitOffsetToPulsar(removeEarliestAndLatest(offset), offsetCommitCallback);
    }

    public void doCommitOffsetToPulsar(
            Map<String, MessageId> offset,
            PulsarCommitCallback offsetCommitCallback) throws InterruptedException {

        try {
            int retries = 0;
            boolean success = false;
            while (running) {
                try {
                    metadataReader.commitCursorToOffset(offset);
                    success = true;
                    break;
                } catch (Exception e) {
                    log.warn("Failed to commit cursor to Pulsar.", e);
                    if (retries >= commitMaxRetries) {
                        log.error("Failed to commit cursor to Pulsar after {} attempts", retries);
                        throw e;
                    }
                    retries += 1;
                    Thread.sleep(1000);
                }
            }
            if (success) {
                offsetCommitCallback.onSuccess();
            } else {
                return;
            }
        } catch (Exception e) {
            if (running) {
                offsetCommitCallback.onException(e);
            } else {
                return;
            }
        }

        for (PulsarTopicState state : subscribedPartitionStates) {
            MessageId off = offset.get(state.getTopic());
            if (off != null) {
                state.setCommittedOffset(off);
            }
        }
    }

    public Map<String, MessageId> removeEarliestAndLatest(Map<String, MessageId> offset) {
        Map<String, MessageId> result = new HashMap<>();
        for (Map.Entry<String, MessageId> entry : offset.entrySet()) {
            MessageId mid = entry.getValue();
            if (!mid.equals(MessageId.earliest) && !mid.equals(MessageId.latest)) {
                result.put(entry.getKey(), mid);
            }
        }
        return result;
    }

    public void addDiscoveredTopics(Set<String> newTopics) throws IOException, ClassNotFoundException {
        List<PulsarTopicState> newStates = createPartitionStateHolders(
                newTopics.stream().collect(Collectors.toMap(t -> t, t -> MessageId.earliest)),
                timestampWatermarkMode,
                watermarksPeriodic,
                watermarksPunctuated,
                userCodeClassLoader);

        for (PulsarTopicState state : newStates) {
            // the ordering is crucial here; first register the state holder, then
            // push it to the partitions queue to be read
            subscribedPartitionStates.add(state);
            unassignedPartitionsQueue.add(state);
        }
    }

    // ------------------------------------------------------------------------
    //  snapshot and restore the state
    // ------------------------------------------------------------------------

    /**
     * Takes a snapshot of the partition offsets.
     *
     * <p>Important: This method must be called under the checkpoint lock.
     *
     * @return A map from partition to current offset.
     */
    public Map<String, MessageId> snapshotCurrentState() {
        // this method assumes that the checkpoint lock is held
        assert Thread.holdsLock(checkpointLock);

        Map<String, MessageId> state = new HashMap<>(subscribedPartitionStates.size());

        for (PulsarTopicState pa : subscribedPartitionStates) {
            state.put(pa.getTopic(), pa.getOffset());
        }
        return state;
    }

    public Map<String, ReaderThread> createAndStartReaderThread(
            List<PulsarTopicState> states,
            ExceptionProxy exceptionProxy) {

        Map<String, MessageId> startingOffsets = states.stream().collect(Collectors.toMap(PulsarTopicState::getTopic, PulsarTopicState::getOffset));
        metadataReader.setupCursor(startingOffsets, failOnDataLoss);
        Map<String, ReaderThread> topic2Threads = new HashMap<>();

        for (PulsarTopicState state : states) {
            ReaderThread<T> readerT = createReaderThread(exceptionProxy, state);
            readerT.setName(String.format("Pulsar Reader for %s in task %s", state.getTopic(), runtimeContext.getTaskName()));
            readerT.setDaemon(true);
            readerT.start();
            log.info("Starting Thread {}", readerT.getName());
            topic2Threads.put(state.getTopic(), readerT);
        }
        return topic2Threads;
    }

    protected List<PulsarTopicState> getSubscribedTopicStates() {
        return subscribedPartitionStates;
    }

    protected ReaderThread createReaderThread(ExceptionProxy exceptionProxy, PulsarTopicState state) {
        log.info("Create reader with failOnDataLoss {}", failOnDataLoss);

        return new ReaderThread(
                this,
                state,
                clientConf,
                readerConf,
                deserializer,
                pollTimeoutMs,
                exceptionProxy, failOnDataLoss);
    }

    /**
     * Utility method that takes the topic partitions and creates the topic partition state
     * holders, depending on the timestamp / watermark mode.
     */
    private List<PulsarTopicState> createPartitionStateHolders(
            Map<String, MessageId> partitionsToInitialOffsets,
            int timestampWatermarkMode,
            SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
            ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

        // CopyOnWrite as adding discovered partitions could happen in parallel
        // while different threads iterate the partitions list
        List<PulsarTopicState> partitionStates = new CopyOnWriteArrayList<>();

        switch (timestampWatermarkMode) {
            case NO_TIMESTAMPS_WATERMARKS: {
                for (Map.Entry<String, MessageId> partitionEntry : partitionsToInitialOffsets.entrySet()) {
                    PulsarTopicState state = new PulsarTopicState(partitionEntry.getKey());
                    state.setOffset(partitionEntry.getValue());
                    partitionStates.add(state);
                }

                return partitionStates;
            }

            case PERIODIC_WATERMARKS: {
                for (Map.Entry<String, MessageId> partitionEntry : partitionsToInitialOffsets.entrySet()) {
                    AssignerWithPeriodicWatermarks<T> assignerInstance = watermarksPeriodic.deserializeValue(userCodeClassLoader);
                    PulsarTopicStateWithPeriodicWatermarks<T> state = new PulsarTopicStateWithPeriodicWatermarks<T>(
                            partitionEntry.getKey(),
                            assignerInstance);
                    state.setOffset(partitionEntry.getValue());
                    partitionStates.add(state);
                }

                return partitionStates;
            }

            case PUNCTUATED_WATERMARKS: {
                for (Map.Entry<String, MessageId> partitionEntry : partitionsToInitialOffsets.entrySet()) {
                    AssignerWithPunctuatedWatermarks<T> assignerInstance = watermarksPunctuated.deserializeValue(userCodeClassLoader);
                    PulsarTopicStateWithPunctuatedWatermarks<T> state = new PulsarTopicStateWithPunctuatedWatermarks<T>(
                            partitionEntry.getKey(),
                            assignerInstance);
                    state.setOffset(partitionEntry.getValue());
                    partitionStates.add(state);
                }

                return partitionStates;
            }

            default:
                // cannot happen, add this as a guard for the future
                throw new RuntimeException();
        }
    }

    /**
     * The periodic watermark emitter. In its given interval, it checks all partitions for
     * the current event time watermark, and possibly emits the next watermark.
     */
    private static class PeriodicWatermarkEmitter implements ProcessingTimeCallback {

        private final List<PulsarTopicState> allPartitions;

        private final SourceContext<?> emitter;

        private final ProcessingTimeService timerService;

        private final long interval;

        private long lastWatermarkTimestamp;

        //-------------------------------------------------

        PeriodicWatermarkEmitter(
                List<PulsarTopicState> allPartitions,
                SourceContext<?> emitter,
                ProcessingTimeService timerService,
                long autoWatermarkInterval) {
            this.allPartitions = checkNotNull(allPartitions);
            this.emitter = checkNotNull(emitter);
            this.timerService = checkNotNull(timerService);
            this.interval = autoWatermarkInterval;
            this.lastWatermarkTimestamp = Long.MIN_VALUE;
        }

        //-------------------------------------------------

        public void start() {
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }

        @Override
        public void onProcessingTime(long timestamp) throws Exception {

            long minAcrossAll = Long.MAX_VALUE;
            boolean isEffectiveMinAggregation = false;
            for (PulsarTopicState state : allPartitions) {

                // we access the current watermark for the periodic assigners under the state
                // lock, to prevent concurrent modification to any internal variables
                final long curr;
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (state) {
                    curr = ((PulsarTopicStateWithPeriodicWatermarks<?>) state).getCurrentWatermarkTimestamp();
                }

                minAcrossAll = Math.min(minAcrossAll, curr);
                isEffectiveMinAggregation = true;
            }

            // emit next watermark, if there is one
            if (isEffectiveMinAggregation && minAcrossAll > lastWatermarkTimestamp) {
                lastWatermarkTimestamp = minAcrossAll;
                emitter.emitWatermark(new Watermark(minAcrossAll));
            }

            // schedule the next watermark
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }
    }

    private static class BreakingException extends Exception {
        static final BreakingException INSTANCE = new BreakingException();

        private BreakingException() {
        }
    }

    public PulsarMetadataReader getMetadataReader() {
        return this.metadataReader;
    }
}
