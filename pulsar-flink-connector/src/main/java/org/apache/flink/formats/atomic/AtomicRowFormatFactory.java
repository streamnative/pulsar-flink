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

package org.apache.flink.formats.atomic;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.table.descriptors.AtomicValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

/**
 * rowFormatFactory for atomic type.
 */
public class AtomicRowFormatFactory extends TableFormatFactoryBase<Row>
        implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {
    public AtomicRowFormatFactory() {
        super(AtomicValidator.FORMAT_TYPE_VALUE, 1, true);
    }

    @Override
    protected List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(AtomicValidator.FORMAT_ATOMIC_SCHEMA);
        properties.add(AtomicValidator.FORMAT_CLASS_NAME);
/*        properties.add(AtomicValidator.FORMAT_SCHEMA);
        properties.add(AtomicValidator.FORMAT_FAIL_ON_MISSING_FIELD);*/
        return properties;
    }

    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        String className = descriptorProperties.getString(AtomicValidator.FORMAT_CLASS_NAME);
        boolean useExtendFields = descriptorProperties.getOptionalBoolean(CONNECTOR + "." + PulsarOptions.USE_EXTEND_FIELD).orElse(false);
        // create and configure
        final AtomicRowDeserializationSchema.Builder builder =
                new AtomicRowDeserializationSchema.Builder(className);
        builder.useExtendFields(useExtendFields);
        return builder.build();
    }

    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        String className = descriptorProperties.getString(AtomicValidator.FORMAT_CLASS_NAME);
        boolean useExtendFields = descriptorProperties.getOptionalBoolean(CONNECTOR + "." + PulsarOptions.USE_EXTEND_FIELD).orElse(false);
        // create and configure
        final AtomicRowSerializationSchema.Builder builder =
                new AtomicRowSerializationSchema.Builder(className);
        builder.useExtendFields(useExtendFields);
        return builder.build();
    }

    private TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
        if (descriptorProperties.containsKey(JsonValidator.FORMAT_SCHEMA)) {
            return (RowTypeInfo) descriptorProperties.getType(JsonValidator.FORMAT_SCHEMA);
        } else if (descriptorProperties.containsKey(JsonValidator.FORMAT_JSON_SCHEMA)) {
            return JsonRowSchemaConverter.convert(descriptorProperties.getString(JsonValidator.FORMAT_JSON_SCHEMA));
        } else {
            return deriveSchema(descriptorProperties.asMap()).toRowType();
        }
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(propertiesMap);

        // validate
        new AtomicValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
