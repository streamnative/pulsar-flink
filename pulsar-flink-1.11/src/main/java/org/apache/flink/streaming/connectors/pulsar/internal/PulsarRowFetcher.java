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

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pulsar Fetcher that specific to Row emitting.
 */
@Slf4j
public class PulsarRowFetcher extends PulsarFetcher<Row> {

    public PulsarRowFetcher(
            SourceFunction.SourceContext<Row> sourceContext,
            Map<String, MessageId> seedTopicsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<Row>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<Row>> watermarksPunctuated,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            StreamingRuntimeContext runtimeContext,
            ClientConfigurationData clientConf,
            Map<String, Object> readerConf,
            int pollTimeoutMs,
            PulsarDeserializationSchema<Row> deserializer,
            PulsarMetadataReader metadataReader) throws Exception {

        super(sourceContext, seedTopicsWithInitialOffsets, watermarksPeriodic, watermarksPunctuated,
                processingTimeProvider, autoWatermarkInterval, userCodeClassLoader, runtimeContext,
                clientConf, readerConf, pollTimeoutMs, deserializer, metadataReader);
    }

    private SchemaInfo getPulsarSchema() {
        try {
            return metadataReader.getPulsarSchema(
                    seedTopicsWithInitialOffsets.keySet().stream().collect(Collectors.toList()));
        } catch (SchemaUtils.IncompatibleSchemaException e) {
            log.error("Incompatible schema encountered while read multi topics {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ReaderThread createReaderThread(ExceptionProxy exceptionProxy, PulsarTopicState state) {
        SchemaInfo schemaInfo = getPulsarSchema();
        return new RowReaderThread(
                this, state, clientConf, readerConf, pollTimeoutMs, schemaInfo, deserializer, null, exceptionProxy);
    }
}
