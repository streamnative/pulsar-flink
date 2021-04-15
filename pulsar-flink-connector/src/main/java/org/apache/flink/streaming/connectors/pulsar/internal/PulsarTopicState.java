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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.MessageId;


/**
 * The state that the Flink Pulsar Source holds for each Pulsar partition.
 */
@Getter
@Setter
public class PulsarTopicState {

    private final TopicRange topicRange;

    private volatile MessageId offset;

    private volatile MessageId committedOffset;

    public PulsarTopicState(String topic) {
        this.topicRange = new TopicRange(topic);
        this.offset = null;
        this.committedOffset = null;
    }

    public PulsarTopicState(String topic, int start, int end) {
        this.topicRange = new TopicRange(topic, start, end);
    }

    public PulsarTopicState(TopicRange topicRange) {
        this.topicRange = topicRange;
    }

    public final boolean isOffsetDefined() {
        return offset != null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("topicRange", getTopicRange())
                .add("offset", isOffsetDefined() ? getOffset().toString() : "not set")
                .toString();
    }
}
