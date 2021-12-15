/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.formats.protobufnative;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.GENERIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC_PATTERN;

/**
 * Support {@link org.apache.pulsar.client.impl.schema.ProtobufNativeSchema} based pulsar schema
 * SchemaRegistry.
 */
@Slf4j
public class PulsarProtobufNativeFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "pulsar-protobuf-native";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        ReadableConfig tableConf = Configuration.fromMap(context.getCatalogTable().getOptions());

        String topic = extractTopicName(context);
        String adminUrl = tableConf.get(ADMIN_URL);
        Properties properties =
                PulsarTableOptions.getPulsarProperties(context.getCatalogTable().getOptions());

        SerializableSupplier<Descriptors.Descriptor> loadDescriptor =
                () -> {
                    SchemaInfo schemaInfo = null;
                    try {
                        PulsarAdmin admin =
                                PulsarClientUtils.newAdminFromConf(adminUrl, properties);
                        schemaInfo = admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    return ((GenericProtobufNativeSchema)
                                    GenericProtobufNativeSchema.of(schemaInfo))
                            .getProtobufNativeSchema();
                };

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                return new PulsarProtobufNativeRowDataDeserializationSchema(
                        loadDescriptor, rowType);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    private String extractTopicName(DynamicTableFactory.Context context) {
        validateTopic(context.getCatalogTable().getOptions());

        String topic;
        if (isGenericTable(context)) {
            topic = context.getCatalogTable().getOptions().get(TOPIC.key());
        } else {
            String database = context.getObjectIdentifier().getDatabaseName();
            String objectName = context.getObjectIdentifier().getObjectName();
            NamespaceName ns = NamespaceName.get(database);
            TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, objectName);
            topic = fullName.toString();
        }
        return topic;
    }

    /**
     * Check if the table is an generic table. A generic table is defined as created using "CREATE
     * TABLE". If the table is generic, when getting the table from catalog there will be an GENERIC
     * option set to True
     *
     * @param context
     * @return true if the topic is created by "CREATE TABLE" , false if the table is directly
     *     mapped from pulsar topic
     */
    private boolean isGenericTable(DynamicTableFactory.Context context) {
        final String isGeneric = context.getCatalogTable().getOptions().get(GENERIC.key());
        return !StringUtils.isNullOrWhitespaceOnly(isGeneric) && Boolean.parseBoolean(isGeneric);
    }

    private void validateTopic(Map<String, String> tableConf) {
        final String topic = tableConf.get(TOPIC.key());
        if (!StringUtils.isNullOrWhitespaceOnly(topic)
                && tableConf.get(TOPIC_PATTERN.key()) != null) {
            throw new IllegalArgumentException(
                    IDENTIFIER + "  format only support single topic, not support topic pattern.");
        }
        if (null != topic && topic.contains(",")) {
            throw new IllegalArgumentException(
                    IDENTIFIER
                            + "  format only support single topic, not support multiple topics.");
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
