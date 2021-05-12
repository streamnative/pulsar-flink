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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.subscription.TopicListSubscriber;
import org.apache.flink.connector.pulsar.source.subscription.TopicPatternSubscriber;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * Pulsar consumer allows a few different ways to consume from the topics, including:
 * <ol>
 * 	 <li>Subscribe from a collection of topics. </li>
 * 	 <li>Subscribe to a topic pattern using Java {@code Regex}. </li>
 * </ol>
 *
 * <p>The PulsarSubscriber provides a unified interface for the Pulsar source to
 * support all these three types of subscribing mode.
 */
@PublicEvolving
public abstract class PulsarSubscriber implements Serializable {
    protected SplitEnumeratorContext<PulsarPartitionSplit> context;

    public void setContext(SplitEnumeratorContext<PulsarPartitionSplit> context) {
        this.context = context;
    }

    /**
     * Get the partitions changes compared to the current partition assignment.
     *
     * <p>Although Pulsar partitions can only expand and will not shrink, the partitions
     * may still disappear when the topic is deleted.
     *
     * @param pulsarAdmin       The pulsar admin used to retrieve partition information.
     * @param currentAssignment the partitions that are currently assigned to the source readers.
     * @return The partition changes compared with the currently assigned partitions.
     */
    public abstract PartitionChange getPartitionChanges(
            PulsarAdmin pulsarAdmin,
            Set<AbstractPartition> currentAssignment) throws PulsarAdminException, InterruptedException, IOException;

    /**
     * A container class to hold the newly added partitions and removed partitions.
     */
    public class PartitionChange {
        private final Set<AbstractPartition> newPartitions;
        private final Set<AbstractPartition> removedPartitions;

        public PartitionChange(Set<AbstractPartition> newPartitions, Set<AbstractPartition> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<AbstractPartition> getNewPartitions() {
            return newPartitions;
        }

        public Set<AbstractPartition> getRemovedPartitions() {
            return removedPartitions;
        }
    }

    // ----------------- factory methods --------------

    public static PulsarSubscriber getTopicListSubscriber(SplitDivisionStrategy splitDivisionStrategy, String... topics) {
        return new TopicListSubscriber(splitDivisionStrategy, topics);
    }

    public static PulsarSubscriber getTopicPatternSubscriber(String namespace, SplitDivisionStrategy splitDivisionStrategy, Set<String> topicPatterns) {
        return new TopicPatternSubscriber(namespace, splitDivisionStrategy, topicPatterns);
    }
}
