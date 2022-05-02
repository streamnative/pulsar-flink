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

import org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;

import org.apache.pulsar.client.api.Range;

import java.util.Objects;

/**
 * Represents pulsar's topic partition.
 */
public class BrokerPartition extends AbstractPartition {
    public static final Range FULL_RANGE = new Range(SerializableRange.FULL_RANGE_START, SerializableRange.FULL_RANGE_END);
    private TopicRange topicRange;

    public BrokerPartition(TopicRange topicRange) {
        super(topicRange.getTopic(), PartitionType.Broker);
        this.topicRange = topicRange;
    }

    public TopicRange getTopicRange() {
        return topicRange;
    }

    public void setTopicRange(TopicRange topicRange) {
        this.topicRange = topicRange;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BrokerPartition that = (BrokerPartition) o;
        return Objects.equals(topicRange, that.topicRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicRange);
    }
}
