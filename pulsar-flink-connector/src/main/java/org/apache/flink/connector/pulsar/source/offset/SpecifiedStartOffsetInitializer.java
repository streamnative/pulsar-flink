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

package org.apache.flink.connector.pulsar.source.offset;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An implementation of {@link StartOffsetInitializer} which initializes the offsets
 * of the partition according to the user specified offsets.
 *
 * <p>Should be initialized through {@link StartOffsetInitializer}.
 */
@Internal
public class SpecifiedStartOffsetInitializer implements StartOffsetInitializer {
    private static final long serialVersionUID = 1649702397250402877L;
    private final Map<AbstractPartition, MessageId> initialOffsets;
    private final MessageId defaultOffset;
    private final boolean inclusive;

    public SpecifiedStartOffsetInitializer(Map<AbstractPartition, MessageId> initialOffsets, MessageId defaultOffset, boolean inclusive) {
        this.initialOffsets = Collections.unmodifiableMap(initialOffsets);
        this.defaultOffset = defaultOffset;
        this.inclusive = inclusive;
    }

    @Override
    public void initializeBeforeCreation(AbstractPartition partition, CreationConfiguration configuration) {
        configuration.getConsumerConfigurationData().setResetIncludeHead(inclusive);
        configuration.setInitialMessageId(initialOffsets.getOrDefault(partition, defaultOffset));
    }

    @Override
    public Optional<String> verifyOffset(
            AbstractPartition partition,
            Supplier<Optional<MessageId>> lastMessageIdFetcher,
            Supplier<Optional<Message<byte[]>>> firstMessageFetcher) {
        MessageId initialId = initialOffsets.getOrDefault(partition, defaultOffset);
        if (initialId.equals(MessageId.earliest) || initialId.equals(MessageId.latest)) {
            return Optional.empty();
        }
        Optional<MessageId> lastMessageId = lastMessageIdFetcher.get();
        if (!lastMessageId.isPresent()) {
            return Optional.of(String.format("Cannot initialize to offset %s because topic is empty", initialId));
        }

        if (initialId.compareTo(lastMessageId.get()) > 0) {
            return Optional.of(String.format("The initial offset %s is beyond the last commit message id", initialId));
        }

        Optional<Message<byte[]>> firstMessage = firstMessageFetcher.get();
        // if it's inclusive, we expect the exact offset
        if (inclusive) {
            if (!firstMessage.isPresent()) {
                return Optional.of(String.format("No data found at offset %s", initialId));
            }
            if (!firstMessage.get().getMessageId().equals(initialId)) {
                return Optional.of(String.format(
                        "Unexpected offset %s, but expected %s",
                        firstMessage.get().getMessageId(),
                        initialId));
            }
        } else if (firstMessage.isPresent()) {
            MessageIdImpl initialIdImpl = (MessageIdImpl) initialId;
            MessageIdImpl nextInitialId = new MessageIdImpl(
                    initialIdImpl.getLedgerId(),
                    initialIdImpl.getEntryId() + 1,
                    initialIdImpl.getPartitionIndex());
            if (!firstMessage.get().getMessageId().equals(nextInitialId)) {
                return Optional.of(String.format(
                        "Unexpected offset %s, but expected %s",
                        firstMessage.get().getMessageId(),
                        nextInitialId));
            }
        }

        return Optional.empty();
    }
}
