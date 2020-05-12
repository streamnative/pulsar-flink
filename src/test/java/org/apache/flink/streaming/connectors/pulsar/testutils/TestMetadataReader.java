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

package org.apache.flink.streaming.connectors.pulsar.testutils;

import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mock for metadata reader.
 */
public class TestMetadataReader extends PulsarMetadataReader {

    private final List<Set<String>> mockGetAllTopicsReturnSequence;

    private int getAllTopicsInvCount = 0;

    public TestMetadataReader(
            Map<String, String> caseInsensitiveParams,
            int indexOfThisSubtask,
            int numParallelSubtasks,
            List<Set<String>> mockGetAllTopicsReturnSequence) throws PulsarClientException {
        super("", new ClientConfigurationData(), "", caseInsensitiveParams, indexOfThisSubtask, numParallelSubtasks);
        this.mockGetAllTopicsReturnSequence = mockGetAllTopicsReturnSequence;
    }

    public Set<String> getTopicPartitionsAll() {
        return mockGetAllTopicsReturnSequence.get(getAllTopicsInvCount++);
    }

    public static List<Set<String>> createMockGetAllTopicsSequenceFromFixedReturn(Set<String> fixed) {
        List<Set<String>> mockSequence = mock(List.class);
        when(mockSequence.get(anyInt())).thenAnswer((Answer<Set<String>>) invocation -> fixed);

        return mockSequence;
    }

    public static List<Set<String>> createMockGetAllTopicsSequenceFromTwoReturns(List<Set<String>> fixed) {
        List<Set<String>> mockSequence = mock(List.class);

        when(mockSequence.get(0)).thenAnswer((Answer<Set<String>>) invocation -> fixed.get(0));
        when(mockSequence.get(1)).thenAnswer((Answer<Set<String>>) invocation -> fixed.get(1));

        return mockSequence;
    }

    public static int getExpectedSubtaskIndex(String tp, int numTasks) {
        if (tp.contains(PulsarOptions.PARTITION_SUFFIX)) {
            int pos = tp.lastIndexOf(PulsarOptions.PARTITION_SUFFIX);
            String topicPrefix = tp.substring(0, pos);
            String topicPartitionIndex = tp.substring(pos + PulsarOptions.PARTITION_SUFFIX.length());
            if (topicPartitionIndex.matches("0|[1-9]\\d*")) {
                int startIndex = (topicPrefix.hashCode() * 31 & Integer.MAX_VALUE) % numTasks;
                return (startIndex + Integer.valueOf(topicPartitionIndex)) % numTasks;
            }
        }
        return (tp.hashCode() * 31 & Integer.MAX_VALUE) % numTasks;
    }
}
