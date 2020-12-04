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

package org.apache.flink.connector.pulsar.source.subscription;

import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.BrokerPartition;
import org.apache.flink.connector.pulsar.source.NoSplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.SplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.util.AsyncUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;

import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class TopicListSubscriber extends AbstractPulsarSubscriber {
    private static final long serialVersionUID = -6917603843104947866L;
    private final SplitDivisionStrategy splitDivisionStrategy;
    private final List<String> topics;

    public TopicListSubscriber(SplitDivisionStrategy splitDivisionStrategy, String... topics) {
        this.splitDivisionStrategy = splitDivisionStrategy;
        checkArgument(topics.length > 0, "At least one topic needs to be specified");
        this.topics = new ArrayList<>(Arrays.asList(topics));
    }

    @Override
    protected Collection<AbstractPartition> getCurrentPartitions(PulsarAdmin pulsarAdmin) throws PulsarAdminException, InterruptedException, IOException {
        Collection<AbstractPartition> partitions = new ArrayList<>();
        try {
            AsyncUtils.parallelAsync(
                    topics,
                    pulsarAdmin.topics()::getPartitionedTopicMetadataAsync,
                    (topic, exception) -> exception.getStatusCode() == 404,
                    (topic, topicMetadata) -> {
                        log.info("in getCurrentPartitions");
                        int numPartitions = topicMetadata.partitions;
                        if (splitDivisionStrategy != NoSplitDivisionStrategy.NO_SPLIT) {
                            // for key-shared mode, one split take over one range for all partitions of one topic.
                            Collection<Range> ranges = splitDivisionStrategy.getRanges(topic, pulsarAdmin, context);
                            if (numPartitions == 0) {
                                for (Range range : ranges) {
                                    partitions.add(new BrokerPartition(new TopicRange(topic, range)));
                                }
                            } else {
                                for (int i = 0; i < numPartitions; i++) {
                                    String fullName = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + i;
                                    for (Range range : ranges) {
                                        partitions.add(new BrokerPartition(new TopicRange(fullName, range)));
                                    }
                                }
                            }
                        } else if (numPartitions == 0) {
                            // non-partitioned and not in key-shared mode, just one split for one topic.
                            partitions.add(new BrokerPartition(new TopicRange(topic, BrokerPartition.FULL_RANGE)));
                        } else {
                            // partitioned and not in key-shared mode, per partition per split for one topic.
                            for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                                String fullName = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + partitionIndex;
                                partitions.add(new BrokerPartition(new TopicRange(fullName, BrokerPartition.FULL_RANGE)));
                                //partitions.add(new Partition(fullName, Partition.AUTO_KEY_RANGE));
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
