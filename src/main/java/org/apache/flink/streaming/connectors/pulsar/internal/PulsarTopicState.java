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

package org.apache.flink.streaming.connectors.pulsar.internal;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.MessageId;

@Getter
@Setter
public class PulsarTopicState {

    private final String topic;

    private volatile MessageId offset;

    private volatile MessageId committedOffset;

    public PulsarTopicState(String topic) {
        this.topic = topic;
        this.offset = null;
        this.committedOffset = null;
    }

    public final boolean isOffsetDefined() {
        return offset != null;
    }

    @Override
    public String toString() {
        return String.format("%s offset = %s",
                topic,
                isOffsetDefined() ? offset.toString() : "not set");
    }
}
