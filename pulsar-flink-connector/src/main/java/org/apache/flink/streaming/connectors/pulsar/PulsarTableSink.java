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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import lombok.EqualsAndHashCode;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Pulsar Table Sink.
 */
@EqualsAndHashCode
public class PulsarTableSink implements AppendStreamTableSink<Row> {

    private final String adminUrl;

    private final TableSchema schema;

    private final Optional<String> defaultTopicName;

    private final ClientConfigurationData clientConf;

    private final Properties properties;

    public PulsarTableSink(String adminUrl, TableSchema schema, Optional<String> defaultTopicName, ClientConfigurationData clientConf, Properties properties) {
        this.adminUrl = checkNotNull(adminUrl);
        this.schema = checkNotNull(schema);
        this.defaultTopicName = defaultTopicName;
        this.clientConf = checkNotNull(clientConf);
        this.properties = checkNotNull(properties);
    }

    public PulsarTableSink(String serviceUrl, String adminUrl, TableSchema schema, Optional<String> defaultTopicName, Properties properties) {
        this(adminUrl, schema, defaultTopicName, new ClientConfigurationData(), properties);
        this.clientConf.setServiceUrl(checkNotNull(serviceUrl));
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        FlinkPulsarRowSink sink = new FlinkPulsarRowSink(adminUrl, defaultTopicName, clientConf, properties, schema.toRowDataType());
        return dataStream
                .addSink(sink)
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(getClass(), getFieldNames()));
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return schema.toRowType();
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }
}
