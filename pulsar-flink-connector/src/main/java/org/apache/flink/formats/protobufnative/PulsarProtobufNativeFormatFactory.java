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

package org.apache.flink.formats.protobufnative;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.table.catalog.ObjectIdentifier;
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
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PROPERTIES;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC_PATTERN;

/**
 * Support {@link org.apache.pulsar.client.impl.schema.ProtobufNativeSchema} based pulsar schema SchemaRegistry.
 */
@Slf4j
public class PulsarProtobufNativeFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "pulsar-protobuf-native";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        ReadableConfig tableConf = Configuration.fromMap(context.getCatalogTable().getOptions());

        String topic = extractTopicName(context);
        String adminUrl = tableConf.get(ADMIN_URL);
        Optional<Map<String, String>> stringMap = tableConf.getOptional(PROPERTIES);
        Properties properties = stringMap.map((map) -> {
            final Properties prop = new Properties();
            prop.putAll(map);
            return prop;
        }).orElse(new Properties());

        SerializableSupplier<Descriptors.Descriptor> loadDescriptor = () -> {
            SchemaInfo schemaInfo = null;
            try {
                PulsarAdmin admin = PulsarClientUtils.newAdminFromConf(adminUrl, properties);
                schemaInfo = admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return ((GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(schemaInfo)).getProtobufNativeSchema();
        };

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                return new PulsarProtobufNativeRowDataDeserializationSchema(loadDescriptor, rowType);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    private String extractTopicName(DynamicTableFactory.Context context) {

        validateTopic(context.getCatalogTable().getOptions());
        String topic = context.getCatalogTable().getOptions().get(TOPIC.key());
        // Maybe catalog mode
        if (StringUtils.isNullOrWhitespaceOnly(topic)) {
            final ObjectIdentifier table = context.getObjectIdentifier();
            topic = TopicName.get(table.getDatabaseName() + "/" + table.getObjectName()).toString();
        }
        return topic;
    }

    private void validateTopic(Map<String, String> tableConf) {
        final String topic = tableConf.get(TOPIC.key());
        if (!StringUtils.isNullOrWhitespaceOnly(topic) && tableConf.get(TOPIC_PATTERN.key()) != null) {
            throw new IllegalArgumentException(IDENTIFIER + "  format only support single topic, not support topic pattern.");
        }
        if (null != topic && topic.contains(",")) {
            throw new IllegalArgumentException(IDENTIFIER + "  format only support single topic, not support multiple topics.");
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
