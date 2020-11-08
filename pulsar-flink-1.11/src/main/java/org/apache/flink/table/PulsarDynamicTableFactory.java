/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

/**
 * Factory for creating configured instances of
 * {@link PulsarDynamicTableFactory}.
 */
public class PulsarDynamicTableFactory implements
        DynamicTableSourceFactory,
        DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        String topic = tableOptions.get(PulsarOptions.TOPIC);
        String adminUrl = tableOptions.get(PulsarOptions.ADMIN_URL);
        String serverUrl = tableOptions.get(PulsarOptions.SERVICE_URL);
        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PulsarOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        PulsarOptions.validateTableOptions(tableOptions);

        Properties properties = removeConnectorPrefix(context.getCatalogTable().toProperties());

        DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new PulsarDynamicTableSink(
                serverUrl,
                adminUrl,
                topic,
                consumedDataType,
                properties,
                encodingFormat
        );
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        String topic = tableOptions.get(PulsarOptions.TOPIC);
        String adminUrl = tableOptions.get(PulsarOptions.ADMIN_URL);
        String serviceUrl = tableOptions.get(PulsarOptions.SERVICE_URL);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PulsarOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        PulsarOptions.validateTableOptions(tableOptions);
        Properties properties = removeConnectorPrefix(context.getCatalogTable().toProperties());

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        final PulsarOptions.StartupOptions startupOptions = PulsarOptions.getStartupOptions(tableOptions, topic);
        return new PulsarDynamicTableSource(
                producedDataType,
                decodingFormat,
                topic,
                serviceUrl,
                adminUrl,
                properties,
                startupOptions
        );
    }

    @Override
    public String factoryIdentifier() {
        return "pulsar";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PulsarOptions.TOPIC);
        options.add(FactoryUtil.FORMAT);
        options.add(PulsarOptions.SERVICE_URL);
        options.add(PulsarOptions.ADMIN_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PulsarOptions.SCAN_STARTUP_MODE);
        options.add(PulsarOptions.SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(PulsarOptions.SCAN_STARTUP_SUB_NAME);

        options.add(PulsarOptions.PULSAR_READER_READER_NAME);
        options.add(PulsarOptions.PULSAR_READER_SUBSCRIPTION_ROLE_PREFIX);
        options.add(PulsarOptions.PULSAR_READER_RECEIVER_QUEUE_SIZE);
        options.add(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MILLIS);

        return options;
    }

    private static Properties removeConnectorPrefix(Map<String, String> in) {
        String connectorPrefix = CONNECTOR + ".";
        Properties out = new Properties();
        for (Map.Entry<String, String> kv : in.entrySet()) {
            String k = kv.getKey();
            String v = kv.getValue();
            if (k.startsWith(connectorPrefix)) {
                out.put(k.substring(connectorPrefix.length()), v);
            } else {
                out.put(k, v);
            }
        }
        return out;
    }
}
