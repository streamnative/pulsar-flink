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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.annotation.PublicEvolving;
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
public interface PulsarSubscriber extends Serializable {

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
    PartitionChange getPartitionChanges(
            PulsarAdmin pulsarAdmin,
            Set<Partition> currentAssignment) throws PulsarAdminException, InterruptedException, IOException;

    /**
     * A container class to hold the newly added partitions and removed partitions.
     */
    class PartitionChange {
        private final Set<Partition> newPartitions;
        private final Set<Partition> removedPartitions;

        public PartitionChange(Set<Partition> newPartitions, Set<Partition> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<Partition> getNewPartitions() {
            return newPartitions;
        }

        public Set<Partition> getRemovedPartitions() {
            return removedPartitions;
        }
    }

    // ----------------- factory methods --------------

    static PulsarSubscriber getTopicListSubscriber(StickyKeyAssigner stickyKeyAssigner, String... topics) {
        return new TopicListSubscriber(stickyKeyAssigner, topics);
    }

    static PulsarSubscriber getTopicPatternSubscriber(String namespace, StickyKeyAssigner stickyKeyAssigner, String... topicPatterns) {
        return new TopicPatternSubscriber(namespace, stickyKeyAssigner, topicPatterns);
    }
}
