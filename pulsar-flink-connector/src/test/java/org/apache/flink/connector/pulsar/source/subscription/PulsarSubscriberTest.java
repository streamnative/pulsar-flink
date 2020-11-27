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

package org.apache.flink.connector.pulsar.source.subscription;

import org.apache.flink.connector.pulsar.source.Partition;
import org.apache.flink.connector.pulsar.source.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.StickyKeyAssigner;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;

import org.apache.pulsar.common.naming.TopicName;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link org.apache.flink.connector.pulsar.source.PulsarSubscriber}.
 */
public class PulsarSubscriberTest extends PulsarTestBase {
    private static final String TOPIC1 = TopicName.get("topic1").toString();
    private static final String TOPIC2 = TopicName.get("pattern-topic").toString();
    private static final String TOPIC1_WITH_PARTITION = TopicName.get("topic1-partition-0").toString();
    private static final String TOPIC2_WITH_PARTITION = TopicName.get("pattern-topic-partition-0").toString();
    private static final Partition assignedPartition1 = new Partition(TOPIC1_WITH_PARTITION, Partition.AUTO_KEY_RANGE);
    private static final Partition assignedPartition2 = new Partition(TOPIC2_WITH_PARTITION, Partition.AUTO_KEY_RANGE);
    private static final Partition removedPartition = new Partition("removed", Partition.AUTO_KEY_RANGE);
    private static final int NUM_PARTITIONS_PER_TOPIC = 10;
    private static final Set<Partition> currentAssignment =
            new HashSet<>(Arrays.asList(assignedPartition1, assignedPartition2, removedPartition));

    @BeforeClass
    public static void setup() throws Exception {
        pulsarAdmin = getPulsarAdmin();
        pulsarClient = getPulsarClient();
        createTestTopic(TOPIC1, NUM_PARTITIONS_PER_TOPIC);
        createTestTopic(TOPIC2, NUM_PARTITIONS_PER_TOPIC);
    }

    @Test
    public void testTopicListSubscriber() throws Exception {
        PulsarSubscriber subscriber =
                PulsarSubscriber.getTopicListSubscriber(StickyKeyAssigner.AUTO, TOPIC1, TOPIC2);
        PulsarSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(pulsarAdmin, currentAssignment);
        Set<Partition> expectedNewPartitions = new HashSet<>(getPartitionsForTopic(TOPIC1));
        expectedNewPartitions.addAll(getPartitionsForTopic(TOPIC2));
        expectedNewPartitions.remove(assignedPartition1);
        expectedNewPartitions.remove(assignedPartition2);
        assertEquals(expectedNewPartitions, change.getNewPartitions());
        assertEquals(Collections.singleton(removedPartition), change.getRemovedPartitions());
    }

    @Test
    public void testTopicPatternSubscriber() throws Exception {
        PulsarSubscriber subscriber = PulsarSubscriber.getTopicPatternSubscriber("public/default", StickyKeyAssigner.AUTO, "persistent://public/default/pattern.*");
        PulsarSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(pulsarAdmin, currentAssignment);

        Set<Partition> expectedNewPartitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            if (!(TOPIC2 + "-partition-" + i).equals(assignedPartition2.getTopic())) {
                expectedNewPartitions.add(new Partition(TOPIC2 + "-partition-" + i, Partition.AUTO_KEY_RANGE));
            }
        }
        Set<Partition> expectedRemovedPartitions =
                new HashSet<>(Arrays.asList(assignedPartition1, removedPartition));

        assertEquals(expectedNewPartitions, change.getNewPartitions());
        assertEquals(expectedRemovedPartitions, change.getRemovedPartitions());
    }

}
