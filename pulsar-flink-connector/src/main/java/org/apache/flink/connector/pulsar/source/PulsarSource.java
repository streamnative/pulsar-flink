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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumerator;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumeratorState;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumeratorStateSerializer;
import org.apache.flink.connector.pulsar.source.reader.ParsedMessage;
import org.apache.flink.connector.pulsar.source.reader.PulsarPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.reader.PulsarRecordEmitter;
import org.apache.flink.connector.pulsar.source.reader.PulsarSourceReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitSerializer;
import org.apache.flink.connector.pulsar.source.util.PulsarAdminUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.shade.com.google.common.io.Closer;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Source implementation of Pulsar.
 *
 * @param <OUT> the output type of the source.
 */
@Internal
public class PulsarSource<OUT>
        implements Source<OUT, PulsarPartitionSplit, PulsarSourceEnumeratorState>, ResultTypeQueryable {
    private static final long serialVersionUID = -8755372893283732098L;
    // Users can choose only one of the following ways to specify the topics to consume from.
    private final PulsarSubscriber subscriber;
    // Users can specify the starting / stopping offset initializer.
    private final StartOffsetInitializer startOffsetInitializer;
    private final StopCondition stopCondition;
    // Boundedness
    private final Boundedness boundedness;
    private final MessageDeserializer<OUT> messageDeserializer;
    // The configurations.
    private final Configuration configuration;
    private final ClientConfigurationData pulsarConfiguration;
    private final ConsumerConfigurationData<byte[]> consumerConfigurationData;
    private final SplitSchedulingStrategy splitSchedulingStrategy;

    private final String adminUrl;
    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;

    public PulsarSource(
            PulsarSubscriber subscriber,
            StartOffsetInitializer startOffsetInitializer,
            StopCondition stopCondition,
            Boundedness boundedness,
            MessageDeserializer<OUT> messageDeserializer,
            Configuration configuration,
            ClientConfigurationData pulsarConfiguration,
            ConsumerConfigurationData<byte[]> consumerConfigurationData,
            SplitSchedulingStrategy splitSchedulingStrategy) {
        this.subscriber = checkNotNull(subscriber);
        this.startOffsetInitializer = checkNotNull(startOffsetInitializer);
        this.stopCondition = checkNotNull(stopCondition);
        this.boundedness = boundedness;
        this.messageDeserializer = checkNotNull(messageDeserializer);
        this.configuration = checkNotNull(configuration);
        this.pulsarConfiguration = checkNotNull(pulsarConfiguration);
        adminUrl = configuration.get(PulsarSourceOptions.ADMIN_URL);
        this.consumerConfigurationData = consumerConfigurationData;
        this.splitSchedulingStrategy = splitSchedulingStrategy;
    }

    /**
     * Get a pulsarSourceBuilder to build a {@link PulsarSource}.
     *
     * @return a Pulsar source builder.
     */
    public static <OUT> PulsarSourceBuilder<OUT> builder() {
        return new PulsarSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public TypeInformation getProducedType() {
        return messageDeserializer.getProducedType();
    }

    @Override
    public SourceReader<OUT, PulsarPartitionSplit> createReader(SourceReaderContext readerContext) {
        FutureNotifier futureNotifier = new FutureNotifier();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<ParsedMessage<OUT>>> elementsQueue =
                new FutureCompletingBlockingQueue<>(futureNotifier);
        ExecutorService listenerExecutor = Executors.newScheduledThreadPool(
                1,
                r -> new Thread(r, "Pulsar listener executor"));
        Closer splitCloser = Closer.create();
        splitCloser.register(listenerExecutor::shutdownNow);
        Supplier<SplitReader<ParsedMessage<OUT>, PulsarPartitionSplit>> splitReaderSupplier = () -> {
            PulsarPartitionSplitReader<OUT> reader = new PulsarPartitionSplitReader<>(
                    configuration,
                    consumerConfigurationData,
                    getClient(),
                    getPulsarAdmin(),
                    messageDeserializer,
                    listenerExecutor);
            splitCloser.register(reader);
            return reader;
        };
        PulsarRecordEmitter<OUT> recordEmitter = new PulsarRecordEmitter<>();

        return new PulsarSourceReader<>(
                futureNotifier,
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                configuration,
                readerContext,
                () -> splitCloser.close());
    }

    @Nonnull
    public PulsarAdmin getPulsarAdmin() {
        if (pulsarAdmin == null) {
            try {
                pulsarAdmin = PulsarAdminUtils.newAdminFromConf(adminUrl, pulsarConfiguration);
            } catch (PulsarClientException e) {
                throw new IllegalStateException("Cannot initialize pulsar admin", e);
            }
        }
        return pulsarAdmin;
    }

    @Nonnull
    public PulsarClient getClient() {
        if (pulsarClient == null) {
            try {
                pulsarClient = CachedPulsarClient.getOrCreate(pulsarConfiguration);
            } catch (PulsarClientException e) {
                throw new IllegalStateException("Cannot initialize pulsar client", e);
            }
        }
        return pulsarClient;
    }

    @Override
    public SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<PulsarPartitionSplit> enumContext) {
        return new PulsarSourceEnumerator(
                subscriber,
                startOffsetInitializer,
                stopCondition,
                getPulsarAdmin(),
                configuration,
                enumContext,
                Collections.emptyMap(),
                splitSchedulingStrategy);
    }

    @Override
    public SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            PulsarSourceEnumeratorState checkpoint) {
        return new PulsarSourceEnumerator(
                subscriber,
                startOffsetInitializer,
                stopCondition,
                getPulsarAdmin(),
                configuration,
                enumContext,
                checkpoint.getCurrentAssignment(),
                splitSchedulingStrategy);
    }

    @Override
    public SimpleVersionedSerializer<PulsarPartitionSplit> getSplitSerializer() {
        return new PulsarPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<PulsarSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new PulsarSourceEnumeratorStateSerializer();
    }
}
