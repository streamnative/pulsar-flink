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

package org.apache.flink.connectors.pulsar.source.subscription;

import org.apache.flink.connectors.pulsar.source.Partition;
import org.apache.flink.connectors.pulsar.source.StickyKeyAssigner;
import org.apache.flink.connectors.pulsar.source.util.AsyncUtils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A subscriber to a fixed list of topics.
 */
public class TopicListSubscriber extends AbstractPulsarSubscriber {
    private static final long serialVersionUID = -6917603843104947866L;
    private final StickyKeyAssigner stickyKeyAssigner;
    private final List<String> topics;

    public TopicListSubscriber(StickyKeyAssigner stickyKeyAssigner, String... topics) {
        this.stickyKeyAssigner = stickyKeyAssigner;
        checkArgument(topics.length > 0, "At least one topic needs to be specified");
        this.topics = new ArrayList<>(Arrays.asList(topics));
    }

    @Override
    protected Collection<Partition> getCurrentPartitions(PulsarAdmin pulsarAdmin) throws PulsarAdminException, InterruptedException, IOException {
        Collection<Partition> partitions = new ArrayList<>();
        try {
            AsyncUtils.parallelAsync(
                    topics,
                    pulsarAdmin.topics()::getPartitionedTopicMetadataAsync,
                    (topic, exception) -> exception.getStatusCode() == 404,
                    (topic, topicMetadata) -> {
                        if (topicMetadata.partitions == 0 || stickyKeyAssigner != StickyKeyAssigner.AUTO) {
                            for (Collection<Range> range : stickyKeyAssigner.getRanges(topic, pulsarAdmin)) {
                                partitions.add(new Partition(topic, range));
                            }
                        } else {
                            for (int partitionIndex = 0; partitionIndex < topicMetadata.partitions; partitionIndex++) {
                                String fullName = TopicName.PARTITIONED_TOPIC_SUFFIX + partitionIndex;
                                partitions.add(new Partition(fullName, Partition.AUTO_KEY_RANGE));
                            }
                        }
                    },
                    PulsarAdminException.class);
        } catch (TimeoutException e) {
            throw new IOException("Cannot retrieve topic metadata: " + e.getMessage());
        }
        return partitions;
    }
}
