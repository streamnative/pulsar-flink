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

package org.apache.flink.connectors.pulsar.source.enumerator;

import org.apache.flink.connectors.pulsar.source.split.PulsarPartitionSplit;

import java.util.List;
import java.util.Map;

public class PulsarSourceEnumeratorState {
    private final Map<Integer, List<PulsarPartitionSplit>> currentAssignment;

    public PulsarSourceEnumeratorState(Map<Integer, List<PulsarPartitionSplit>> currentAssignment) {
        this.currentAssignment = currentAssignment;
    }

    public Map<Integer, List<PulsarPartitionSplit>> getCurrentAssignment() {
        return currentAssignment;
    }
}
