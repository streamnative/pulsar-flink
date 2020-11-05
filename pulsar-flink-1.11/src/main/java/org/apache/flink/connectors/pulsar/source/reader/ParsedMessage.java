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

package org.apache.flink.connectors.pulsar.source.reader;

import org.apache.pulsar.client.api.MessageId;

import java.util.Objects;

/**
 * Represents the parsed message.
 */
public class ParsedMessage<T> {
    private final T payload;
    private final MessageId messageId;
    private final long timestamp;

    public ParsedMessage(T payload, MessageId messageId, long timestamp) {
        this.payload = payload;
        this.messageId = messageId;
        this.timestamp = timestamp;
    }

    public T getPayload() {
        return payload;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
        final ParsedMessage<?> that = (ParsedMessage<?>) o;
        return timestamp == that.timestamp &&
                payload.equals(that.payload) &&
                messageId.equals(that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, messageId, timestamp);
    }

    @Override
    public String toString() {
        return "ParsedMessage{" +
                "payload=" + payload +
                ", messageId=" + messageId +
                ", timestamp=" + timestamp +
                '}';
    }
}
