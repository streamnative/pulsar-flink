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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarFetcher;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarRowFetcher;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Emit Pulsar message as Row to Flink.
 */
@Slf4j
public class FlinkPulsarRowSource extends FlinkPulsarSource<Row> {

    private TypeInformation<Row> typeInformation;

    public FlinkPulsarRowSource(String adminUrl, ClientConfigurationData clientConf, Properties properties) {
        super(adminUrl, clientConf, (PulsarDeserializationSchema<Row>) null, properties);
    }

    public FlinkPulsarRowSource(String serviceUrl, String adminUrl, Properties properties) {
        super(serviceUrl, adminUrl, (PulsarDeserializationSchema<Row>) null, properties);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        if (typeInformation == null) {
            try (PulsarMetadataReader reader = new PulsarMetadataReader(adminUrl, clientConfigurationData, "", caseInsensitiveParams, -1, -1)) {
                List<String> topics = reader.getTopics();
                FieldsDataType schema = reader.getSchema(topics);
                typeInformation = (TypeInformation<Row>) LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(schema);
            } catch (Exception e) {
                log.error("Failed to get schema for source with exception {}", ExceptionUtils.stringifyException(e));
                typeInformation = null;
            }
        }

        return typeInformation;
    }

    @Override
    protected PulsarFetcher<Row> createFetcher(
            SourceContext sourceContext,
            Map<String, MessageId> seedTopicsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<Row>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<Row>> watermarksPunctuated,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            StreamingRuntimeContext streamingRuntime) throws Exception {

        return new PulsarRowFetcher(
                sourceContext,
                seedTopicsWithInitialOffsets,
                watermarksPeriodic,
                watermarksPunctuated,
                processingTimeProvider,
                autoWatermarkInterval,
                userCodeClassLoader,
                streamingRuntime,
                clientConfigurationData,
                readerConf,
                pollTimeoutMs,
                null,
				metadataReader);
    }
}
