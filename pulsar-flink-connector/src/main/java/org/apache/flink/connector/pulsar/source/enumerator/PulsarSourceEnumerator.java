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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.SplitSchedulingStrategy;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.StopCondition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.ComponentClosingUtils.closeWithTimeout;

/**
 * The enumerator class for pulsar source.
 */
public class PulsarSourceEnumerator implements SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceEnumerator.class);
    private final PulsarSubscriber subscriber;
    private final StartOffsetInitializer startOffsetInitializer;
    private final StopCondition stopCondition;
    private final PulsarAdmin pulsarAdmin;
    private final Configuration configuration;
    private final long partitionDiscoveryIntervalMs;
    private final SplitEnumeratorContext<PulsarPartitionSplit> context;

    // The internal states of the enumerator.
    /**
     * This set is only accessed by the partition discovery callable in the callAsync() method, i.e worker thread.
     */
    private final Set<AbstractPartition> discoveredPartitions;
    /**
     * The current assignment by reader id. Only accessed by the coordinator thread.
     */
    private final Map<Integer, List<PulsarPartitionSplit>> readerIdToSplitAssignments;
    /**
     * The discovered and initialized partition splits that are waiting for owner reader to be ready.
     */
    private final Map<Integer, List<PulsarPartitionSplit>> pendingPartitionSplitAssignment;

    // Lazily instantiated or mutable fields.
    private boolean noMoreNewPartitionSplits = false;

    private SplitSchedulingStrategy splitSchedulingStrategy;

    public PulsarSourceEnumerator(
            PulsarSubscriber subscriber,
            StartOffsetInitializer startOffsetInitializer,
            StopCondition stopCondition,
            PulsarAdmin pulsarAdmin,
            Configuration configuration,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            Map<Integer, List<PulsarPartitionSplit>> currentSplitsAssignments,
            SplitSchedulingStrategy splitSchedulingStrategy) {
        this.subscriber = subscriber;
        this.subscriber.setContext(context);
        this.startOffsetInitializer = startOffsetInitializer;
        this.stopCondition = stopCondition;
        this.pulsarAdmin = pulsarAdmin;
        this.configuration = configuration;
        this.context = context;
        this.splitSchedulingStrategy = splitSchedulingStrategy;
        discoveredPartitions = new HashSet<>();
        readerIdToSplitAssignments = new HashMap<>(currentSplitsAssignments);
        readerIdToSplitAssignments.forEach((reader, splits) ->
                splits.forEach(s -> discoveredPartitions.add(s.getPartition())));
        pendingPartitionSplitAssignment = new HashMap<>();
        //readerIdTowaitSplits = new HashMap<>();
        partitionDiscoveryIntervalMs = configuration.get(PulsarSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
    }

    @Override
    public void start() {
        if (partitionDiscoveryIntervalMs > 0) {
            context.callAsync(
                    this::discoverAndInitializePartitionSplit,
                    this::handlePartitionSplitChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            context.callAsync(
                    this::discoverAndInitializePartitionSplit,
                    this::handlePartitionSplitChanges);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {

    }

    @Override
    public void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId) {
        splitSchedulingStrategy.addSplitsBack(pendingPartitionSplitAssignment, splits, subtaskId, context.currentParallelism());
        assignPendingPartitionSplits();
    }

    //id -> 5
    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader {}.", subtaskId);
        assignPendingPartitionSplits();
    }

    @Override
    public PulsarSourceEnumeratorState snapshotState() throws Exception {
        return new PulsarSourceEnumeratorState(readerIdToSplitAssignments);
    }

    @Override
    public void close() {
        // When close the split enumerator, we need to make sure that the async calls are canceled
        // before we can close the consumer and admin clients.
        long closeTimeoutMs = configuration.get(PulsarSourceOptions.CLOSE_TIMEOUT_MS);
        close(closeTimeoutMs).ifPresent(t -> LOG.warn("Encountered error when closing PulsarSourceEnumerator", t));
    }

    @VisibleForTesting
    Optional<Throwable> close(long timeoutMs) {
        return closeWithTimeout(
                "PulsarSourceEnumerator",
                (ThrowingRunnable<Exception>) () -> pulsarAdmin.close(),
                timeoutMs);
    }

    // ----------------- private methods -------------------

    private PartitionSplitChange discoverAndInitializePartitionSplit() throws IOException, InterruptedException, PulsarAdminException {
        // Make a copy of the partitions to owners
        PulsarSubscriber.PartitionChange partitionChange =
                subscriber.getPartitionChanges(pulsarAdmin, Collections.unmodifiableSet(discoveredPartitions));

        discoveredPartitions.addAll(partitionChange.getNewPartitions());

        List<PulsarPartitionSplit> partitionSplits = partitionChange.getNewPartitions().stream()
                .map(partition -> new PulsarPartitionSplit(partition, startOffsetInitializer, stopCondition))
                .collect(Collectors.toList());
        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handlePartitionSplitChanges(PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to handle partition splits change due to ", t);
        }
        if (partitionDiscoveryIntervalMs < 0) {
            noMoreNewPartitionSplits = true;
        }
        // TODO: Handle removed partitions.
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.newPartitionSplits);
        assignPendingPartitionSplits();
    }

    // This method should only be invoked in the coordinator executor thread.
    // map(range, subid)
    private void addPartitionSplitChangeToPendingAssignments(Collection<PulsarPartitionSplit> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        for (PulsarPartitionSplit split : newPartitionSplits) {
            // TODO: Implement a more sophisticated algorithm to reduce partition movement when parallelism changes.
            pendingPartitionSplitAssignment.computeIfAbsent(
                    splitSchedulingStrategy.getIndexOfReader(numReaders, split), r -> new ArrayList<>()
            ).add(split);
        }
        LOG.debug("Assigned {} to {} readers.", newPartitionSplits, numReaders);
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingPartitionSplits() {
        Map<Integer, List<PulsarPartitionSplit>> incrementalAssignment = new HashMap<>();
        pendingPartitionSplitAssignment.forEach((ownerReader, pendingSplits) -> {
            if (!pendingSplits.isEmpty() && context.registeredReaders().containsKey(ownerReader)) {
                // The owner reader is ready, assign the split to the owner reader.
                incrementalAssignment.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(pendingSplits);
            }
        });
        if (incrementalAssignment.isEmpty()) {
            // No assignment is made.
            return;
        }
        context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        incrementalAssignment.forEach((readerOwner, newPartitionSplits) -> {
            // Update the split assignment.
            readerIdToSplitAssignments.computeIfAbsent(readerOwner, r -> new ArrayList<>()).addAll(newPartitionSplits);
            // Clear the pending splits for the reader owner.
            pendingPartitionSplitAssignment.remove(readerOwner);
            // Sends NoMoreSplitsEvent to the readers if there is no more partition splits to be assigned.
            if (noMoreNewPartitionSplits) {
                context.sendEventToSourceReader(readerOwner, new NoMoreSplitsEvent());
            }
        });
    }

    /**
     * class that represents partitionSplit's change.
     */
    public static class PartitionSplitChange {
        private final List<PulsarPartitionSplit> newPartitionSplits;
        private final Set<AbstractPartition> removedPartitions;

        private PartitionSplitChange(
                List<PulsarPartitionSplit> newPartitionSplits,
                Set<AbstractPartition> removedPartitions) {
            this.newPartitionSplits = newPartitionSplits;
            this.removedPartitions = removedPartitions;
        }
    }
}
