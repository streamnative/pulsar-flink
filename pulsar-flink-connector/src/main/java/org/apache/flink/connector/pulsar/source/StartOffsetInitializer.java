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
import org.apache.flink.connector.pulsar.source.offset.ExternalSubscriptionStartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.offset.RollbackStartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.offset.SpecifiedStartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.offset.TimestampStartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A interface for users to specify the starting / stopping offset of a {@link PulsarPartitionSplit}.
 */
@PublicEvolving
public interface StartOffsetInitializer extends Serializable {

    /**
     * Initializes the offset for the given consumer configuration and partition.
     *
     * @param partition     the partition for which the consumer is about to be created
     * @param configuration the configuration used to create consumer
     */
    default void initializeBeforeCreation(AbstractPartition partition, CreationConfiguration configuration) {
    }

    /**
     * Initializes the offset for the given consumer and partition.
     *
     * @param partition the partition for which the consumer has been created
     * @param consumer  the consumer
     */
    default void initializeAfterCreation(AbstractPartition partition, Consumer<?> consumer) throws PulsarClientException {
    }

    /**
     * Verifies if the offset was initialized correctly.
     *
     * @return an error message if no appropriate data point could be found or empty if initialization worked correctly.
     * @see PulsarSourceOptions#VERIFY_INITIAL_OFFSETS
     */
    default Optional<String> verifyOffset(
            AbstractPartition partition,
            Supplier<Optional<MessageId>> lastMessageIdFetcher,
            Supplier<Optional<Message<byte[]>>> firstMessageFetcher) {
        return Optional.empty();
    }

    // --------------- factory methods ---------------

    static StartOffsetInitializer committedOffsets(String subscriptionName) {
        return committedOffsets(subscriptionName, MessageId.earliest);
    }

    static StartOffsetInitializer committedOffsets(String subscriptionName, MessageId defaultOffset) {
        return new ExternalSubscriptionStartOffsetInitializer(subscriptionName, defaultOffset);
    }

    static StartOffsetInitializer timestamps(long timestamp) {
        return new TimestampStartOffsetInitializer(timestamp);
    }

    static StartOffsetInitializer rollback(long rollbackDuration, TimeUnit timeUnit) {
        return new RollbackStartOffsetInitializer(timeUnit.toSeconds(rollbackDuration));
    }

    static StartOffsetInitializer earliest() {
        return earliest(true);
    }

    static StartOffsetInitializer earliest(boolean inclusive) {
        return new SpecifiedStartOffsetInitializer(Collections.emptyMap(), MessageId.earliest, inclusive);
    }

    static StartOffsetInitializer latest() {
        return latest(true);
    }

    static StartOffsetInitializer latest(boolean inclusive) {
        return new SpecifiedStartOffsetInitializer(Collections.emptyMap(), MessageId.latest, inclusive);
    }

    static StartOffsetInitializer offset(MessageId offset, boolean inclusive) {
        return offsets(Collections.emptyMap(), offset, inclusive);
    }

    static StartOffsetInitializer offsets(Map<AbstractPartition, MessageId> offsets) {
        return offsets(offsets, MessageId.earliest, true);
    }

    static StartOffsetInitializer offsets(Map<AbstractPartition, MessageId> offsets, MessageId defaultOffset, boolean inclusive) {
        return new SpecifiedStartOffsetInitializer(offsets, defaultOffset, inclusive);
    }

    /**
     * config class to create consumer.
     */
    class CreationConfiguration {
        private final ConsumerConfigurationData<byte[]> consumerConfigurationData;
        @Nullable
        private MessageId initialMessageId;
        private long rollbackInS = 0;

        public CreationConfiguration(ConsumerConfigurationData<byte[]> consumerConfigurationData) {
            this.consumerConfigurationData = consumerConfigurationData;
        }

        public ConsumerConfigurationData<byte[]> getConsumerConfigurationData() {
            return consumerConfigurationData;
        }

        @Nullable
        public MessageId getInitialMessageId() {
            return initialMessageId;
        }

        public void setInitialMessageId(@Nullable MessageId initialMessageId) {
            this.initialMessageId = initialMessageId;
        }

        public long getRollbackInS() {
            return rollbackInS;
        }

        public void setRollbackInS(long rollbackInS) {
            this.rollbackInS = rollbackInS;
        }
    }
}
