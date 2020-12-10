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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SplitSchedulingStrategy for Hash mode.
 */
public class HashSplitSchedulingStrategy implements SplitSchedulingStrategy {
    public static final HashSplitSchedulingStrategy INSTANCE = new HashSplitSchedulingStrategy();

    private HashSplitSchedulingStrategy() {}

    @Override
    public int getIndexOfReader(int numReaders, PulsarPartitionSplit split) {
        //must use positive hashcode.
        return ((split.getTopic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;
    }

    @Override
    public void addSplitsBack(
            Map<Integer, List<PulsarPartitionSplit>> pendingPartitionSplitAssignment,
            List<PulsarPartitionSplit> splits,
            int subtaskId,
            int numReaders) {
        for (PulsarPartitionSplit split : splits) {
            pendingPartitionSplitAssignment.computeIfAbsent(
                    getIndexOfReader(numReaders, split), r -> new ArrayList<>()
            ).add(split);
        }
    }
}
