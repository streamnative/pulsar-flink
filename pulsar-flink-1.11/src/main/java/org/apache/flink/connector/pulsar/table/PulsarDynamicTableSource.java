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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchemaWrapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET;
import static org.apache.flink.table.descriptors.PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;

/**
 * pulsar dynamic table source.
 */
@Slf4j
public class PulsarDynamicTableSource implements ScanTableSource {

    // --------------------------------------------------------------------------------------------
    // Common attributes
    // --------------------------------------------------------------------------------------------
    protected final DataType outputDataType;

    // --------------------------------------------------------------------------------------------
    // Scan format attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Scan format for decoding records from Pulsar.
     */
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    // --------------------------------------------------------------------------------------------
    // Pulsar-specific attributes
    // --------------------------------------------------------------------------------------------

    /**
     * The Pulsar topic to consume.
     */
    protected final List<String> topics;

    /**
     * The Pulsar topic to consume.
     */
    protected final String topicPattern;

    /**
     * The Pulsar topic to consume.
     */
    protected final String serviceUrl;

    /**
     * The Pulsar topic to consume.
     */
    protected final String adminUrl;

    /**
     * Properties for the Pulsar consumer.
     */
    protected final Properties properties;

    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#LATEST}).
     */
    protected final PulsarOptions.StartupOptions startupOptions;

    /**
     * The default value when startup timestamp is not used.
     */
    private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

    public PulsarDynamicTableSource(DataType outputDataType,
                                    DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                    List<String> topics,
                                    String topicPattern,
                                    String serviceUrl,
                                    String adminUrl,
                                    Properties properties,
                                    PulsarOptions.StartupOptions startupOptions) {
        this.outputDataType = outputDataType;
        this.decodingFormat = decodingFormat;
        this.topics = topics;
        this.topicPattern = topicPattern;
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
        setTopicInfo(properties, topics, topicPattern);
        this.properties = properties;
        this.startupOptions = startupOptions;
    }

    private void setTopicInfo(Properties properties, List<String> topics, String topicPattern) {
        if (StringUtils.isNotBlank(topicPattern)){
            properties.putIfAbsent("topicspattern", topicPattern);
            properties.remove("topic");
            properties.remove("topics");
        } else if (topics != null && topics.size() > 1) {
            properties.putIfAbsent("topics", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topic");
        } else if (topics != null && topics.size() == 1){
            properties.putIfAbsent("topic", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topics");
        } else {
            throw new RuntimeException("Use `topics` instead of `topic` for multi topic read");
        }
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        DeserializationSchema<RowData> deserializationSchema =
                this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.outputDataType);

        FlinkPulsarSource<RowData> source = new FlinkPulsarSource<>(
                adminUrl,
                newClientConf(serviceUrl),
                new PulsarDeserializationSchemaWrapper<>(deserializationSchema),
                properties
        );
        // TODO 调整结构
        switch (startupOptions.startupMode) {
            case EARLIEST:
                source.setStartFromEarliest();
                break;
            case LATEST:
                source.setStartFromLatest();
                break;
            case SPECIFIC_OFFSETS:
                source.setStartFromSpecificOffsets(startupOptions.specificOffsets);
                break;
            case EXTERNAL_SUBSCRIPTION:
                MessageId subscriptionPosition = MessageId.latest;
                if (CONNECTOR_STARTUP_MODE_VALUE_EARLIEST
                        .equals(properties.get(CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET))) {
                    subscriptionPosition = MessageId.earliest;
                }
                source.setStartFromSubscription(startupOptions.externalSubscriptionName, subscriptionPosition);
        }
        return SourceFunctionProvider.of(source, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new PulsarDynamicTableSource(
                this.outputDataType,
                this.decodingFormat,
                this.topics,
                this.topicPattern,
                this.serviceUrl,
                this.adminUrl,
                this.properties,
                this.startupOptions
        );
    }

    @Override
    public String asSummaryString() {
        return "Pulsar universal table source";
    }

    private static ClientConfigurationData newClientConf(String serviceUrl) {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(serviceUrl);
        return clientConf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PulsarDynamicTableSource)) {
            return false;
        }
        PulsarDynamicTableSource that = (PulsarDynamicTableSource) o;
        return outputDataType.equals(that.outputDataType) &&
                decodingFormat.equals(that.decodingFormat) &&
                Objects.equals(topics, that.topics) &&
                Objects.equals(topicPattern, that.topicPattern) &&
                serviceUrl.equals(that.serviceUrl) &&
                adminUrl.equals(that.adminUrl) &&
                //The properties generated by flink can't be compared to your own strength for now,
                // because the content is always a bit different.
                // properties.equals(that.properties) &&
                startupOptions.equals(that.startupOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputDataType, decodingFormat, topics, topicPattern, serviceUrl, adminUrl, startupOptions);
    }
}
