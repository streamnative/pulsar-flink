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
import org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SplitSchedulingStrategy for keyShared mode.
 */
public class KeySharedSplitSchedulingStrategy implements SplitSchedulingStrategy {
    public static final KeySharedSplitSchedulingStrategy INSTANCE = new KeySharedSplitSchedulingStrategy();

    private Map<SerializableRange, Integer> rangeToReaders;
    private int nextId = 0;

    private KeySharedSplitSchedulingStrategy() {
        this.rangeToReaders = new HashMap<>();
    }

    @Override
    public int getIndexOfReader(int numReaders, PulsarPartitionSplit split) {
        BrokerPartition partition = (BrokerPartition) split.getPartition();
        SerializableRange pulsarRange = partition.getTopicRange().getRange();
        return rangeToReaders.computeIfAbsent(pulsarRange, serializableRange -> {
            rangeToReaders.put(serializableRange, nextId);
            int readerId = nextId;
            nextId++;
            return readerId;
        });
    }

    @Override
    public void addSplitsBack(Map<Integer, List<PulsarPartitionSplit>> pendingPartitionSplitAssignment,
                              List<PulsarPartitionSplit> splits,
                              int subtaskId,
                              int numReaders) {
        pendingPartitionSplitAssignment.computeIfAbsent(subtaskId, r -> new ArrayList<>()).addAll(splits);
    }
}
