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

package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.pulsar.common.ConnectorConfig;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.shade.com.google.common.collect.Maps;
import org.apache.pulsar.shade.io.netty.buffer.ByteBufUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Read Pulsar data in batch mode.
 * Bypass broker and read segments directly for efficiency.
 * @param <T> the type of record.
 */
@Slf4j
@ToString
public class PulsarInputFormat<T> extends RichInputFormat<T, PulsarInputSplit> implements ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    // global fields

    private ConnectorConfig connectorConfig;

    private DeserializationSchema<T> deserializer;

    // instance wise fields

    private transient Configuration parameters;

    private transient InputSplitReader<T> reader;

    public PulsarInputFormat(
            ConnectorConfig connectorConfig,
            DeserializationSchema<T> deserializer) {
        this.connectorConfig = connectorConfig;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Configuration parameters) {
        this.parameters = parameters;
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public PulsarInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        List<PulsarInputSplit> pulsarSplits = new ArrayList<>();

//		String adminUrl,
//		ClientConfigurationData clientConf,
//		String subscriptionName,
//		Map<String, String> caseInsensitiveParams,
//		int indexOfThisSubtask,
//		int numParallelSubtasks,
//		boolean useExternalSubscription
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setAuthPluginClassName(connectorConfig.getAuthPluginClassName());
        clientConf.setAuthParams(connectorConfig.getAuthParams());
        clientConf.setServiceUrl(connectorConfig.getServiceUrl());
        final Map<String, String> caseInsensitiveParams = Maps.newHashMap();

        Optional.ofNullable(connectorConfig.getTopic())
                .ifPresent(value -> caseInsensitiveParams.put(PulsarOptions.TOPIC_SINGLE_OPTION_KEY, value));
        Optional.ofNullable(connectorConfig.getTopics())
                .ifPresent(value -> caseInsensitiveParams.put(PulsarOptions.TOPIC_MULTI_OPTION_KEY, value));
        Optional.ofNullable(connectorConfig.getTopicsPattern())
                .ifPresent(value -> caseInsensitiveParams.put(PulsarOptions.TOPIC_PATTERN_OPTION_KEY, value));
        try (PulsarMetadataReader reader = new PulsarMetadataReader(
                connectorConfig.getAdminUrl(),
                clientConf,
                "",
                caseInsensitiveParams,
                -1,
                -1

        )) {

            Set<String> topics = reader.getTopicPartitionsAll();

            List<InputLedger> allLegers = new ArrayList<>();

            for (String topic : topics) {
                Collection<InputLedger> ledgers = SplitUtils.getLedgersInBetween(
                        topic,
                        (MessageIdImpl) MessageId.earliest,
                        (MessageIdImpl) MessageId.latest,
                        CachedClients.getInstance(connectorConfig));
                allLegers.addAll(ledgers);
            }

            List<List<InputLedger>> ldSplits =
                    SplitUtils.partitionToNSplits(allLegers, connectorConfig.getTargetNumSplits());

            for (int i = 0; i < ldSplits.size(); i++) {
                pulsarSplits.add(genSplit(i, ldSplits.get(i)));
            }

            return pulsarSplits.toArray(new PulsarInputSplit[0]);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    protected PulsarInputSplit genSplit(int index, List<InputLedger> ledgers) {
        return new PulsarInputSplit(index, ledgers);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(PulsarInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(PulsarInputSplit split) throws IOException {
        try {
            reader = new GenericSplitReader(connectorConfig, split.getSplitNumber(), split.getLedgersToRead());
        } catch (Exception e) {
            throw new IOException(String.format("Failed to open split %d to read", split.getSplitNumber()), e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !reader.next();
    }

    @Override
    public T nextRecord(T reuse) throws IOException {
        return reader.get();
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                throw new IOException();
            }
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    private class GenericSplitReader extends InputSplitReader {

        public GenericSplitReader(ConnectorConfig connectorConfig, int partitionId, List<InputLedger> ledgersToRead)
                throws Exception {
            super(connectorConfig, partitionId, ledgersToRead);
        }

        @Override
        public T deserialize(RawMessage currentMessage) throws IOException {
            return deserializer.deserialize(ByteBufUtil.getBytes(currentMessage.getData()));
        }
    }
}
