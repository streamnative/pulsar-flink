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

import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * An implementation of {@link StartOffsetInitializer} to rollback the offsets
 * by a certain duration.
 *
 * <p>This implementation does not verify if data exists after the rollback as this initiliazer
 *
 * <p>Package private and should be instantiated via {@link StartOffsetInitializer}.
 */
public class RollbackStartOffsetInitializer implements StartOffsetInitializer {
    private static final long serialVersionUID = 2932230571773627233L;
    private final long rollbackTimeInS;

    public RollbackStartOffsetInitializer(long rollbackTimeInS) {
        this.rollbackTimeInS = rollbackTimeInS;
    }

    @Override
    public void initializeBeforeCreation(AbstractPartition partition, CreationConfiguration creationConfiguration) {
        creationConfiguration.setRollbackInS(rollbackTimeInS);
    }

    @Override
    public Optional<String> verifyOffset(
            AbstractPartition partition,
            Supplier<Optional<MessageId>> lastMessageIdFetcher,
            Supplier<Optional<Message<byte[]>>> firstMessageFetcher) {
        return firstMessageFetcher.get().isPresent() ?
                Optional.empty() :
                Optional.of(String.format("No data found %s secs ago", rollbackTimeInS));
    }
}
