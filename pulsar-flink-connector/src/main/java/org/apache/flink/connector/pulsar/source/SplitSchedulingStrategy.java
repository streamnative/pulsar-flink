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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An interface to determine which subtask to execute the split on.
 */
public interface SplitSchedulingStrategy extends Serializable {
    int getIndexOfReader(int numReaders, PulsarPartitionSplit split);

    void addSplitsBack(
            Map<Integer, List<PulsarPartitionSplit>> pendingPartitionSplitAssignment,
            List<PulsarPartitionSplit> splits,
            int subtaskId,
            int numReaders);
}
