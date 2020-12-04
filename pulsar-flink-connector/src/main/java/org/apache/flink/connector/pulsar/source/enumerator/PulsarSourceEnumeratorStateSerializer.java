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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitSerializer;
import org.apache.flink.connector.pulsar.source.util.SerdeUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the enumerator state of
 * Kafka source.
 */
public class PulsarSourceEnumeratorStateSerializer implements SimpleVersionedSerializer<PulsarSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PulsarSourceEnumeratorState enumState) throws IOException {
        return SerdeUtils.serializeSplitAssignments(
                enumState.getCurrentAssignment(),
                new PulsarPartitionSplitSerializer());
    }

    @Override
    public PulsarSourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version == 0) {
            Map<Integer, List<PulsarPartitionSplit>> currentPartitionAssignment = SerdeUtils.deserializeSplitAssignments(
                    serialized,
                    new PulsarPartitionSplitSerializer(),
                    ArrayList::new);
            return new PulsarSourceEnumeratorState(currentPartitionAssignment);
        }
        throw new IOException(String.format("The bytes are serialized with version %d, " +
                "while this deserializer only supports version up to %d", version, CURRENT_VERSION));
    }
}
