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

package org.apache.flink.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.TopicKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Optional;
import java.util.Properties;

/**
 * pulsar dynamic table sink.
 */
public class PulsarDynamicTableSink implements DynamicTableSink {

    /**
     * Consumed data type of the table.
     */
    protected final DataType consumedDataType;

    /**
     * The pulsar topic to write to.
     */
    protected final String topic;
    protected final String serviceUrl;
    protected final String adminUrl;

    /**
     * Properties for the pulsar producer.
     */
    protected final Properties properties;

    /**
     * Sink format for encoding records to pulsar.
     */
    protected final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    protected PulsarDynamicTableSink(
            String serviceUrl,
            String adminUrl,
            String topic,
            DataType consumedDataType,
            Properties properties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        this.serviceUrl = Preconditions.checkNotNull(serviceUrl, "serviceUrl data type must not be null.");
        this.adminUrl = Preconditions.checkNotNull(adminUrl, "adminUrl data type must not be null.");
        this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.consumedDataType = Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.encodingFormat = Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                this.encodingFormat.createRuntimeEncoder(context, this.consumedDataType);
        final SinkFunction<RowData> pulsarSink = createPulsarSink(
                this.topic,
                this.properties,
                serializationSchema);

        return SinkFunctionProvider.of(pulsarSink);
    }

    private SinkFunction<RowData> createPulsarSink(String topic, Properties properties,
                                                   SerializationSchema<RowData> serializationSchema) {

        return new FlinkPulsarSink<RowData>(
                serviceUrl,
                adminUrl,
                Optional.ofNullable(topic),
                properties,
                TopicKeyExtractor.NULL,
                RowData.class,
                RecordSchemaType.AVRO
        );
    }

    @Override
    public DynamicTableSink copy() {
        return new PulsarDynamicTableSink(
                this.serviceUrl,
                this.adminUrl,
                this.topic,
                this.consumedDataType,
                this.properties,
                this.encodingFormat
        );
    }

    @Override
    public String asSummaryString() {
        return "Pulsar universal table sink";
    }
}
