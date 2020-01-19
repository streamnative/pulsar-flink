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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.RowtimeValidator;
import org.apache.flink.table.descriptors.SchemaValidator;

import java.util.Map;

import static org.apache.flink.table.descriptors.Rowtime.ROWTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Pulsar validator that allows to only specify protime or rowtime in schema.
 * Other fields could be inferred from Pulsar schema.
 */
public class PulsarSchemaValidator extends SchemaValidator {

    private final boolean isStreamEnvironment;
    private final boolean supportsSourceTimestamps;
    private final boolean supportsSourceWatermarks;

    public PulsarSchemaValidator(boolean isStreamEnvironment, boolean supportsSourceTimestamps, boolean supportsSourceWatermarks) {
        super(isStreamEnvironment, supportsSourceTimestamps, supportsSourceWatermarks);
        this.isStreamEnvironment = isStreamEnvironment;
        this.supportsSourceTimestamps = supportsSourceTimestamps;
        this.supportsSourceWatermarks = supportsSourceWatermarks;
    }

    @Override
    public void validate(DescriptorProperties properties) {
        Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);
        Map<String, String> types = properties.getIndexedProperty(SCHEMA, SCHEMA_TYPE);

        boolean proctimeFound = false;

        int fieldsCount = Math.max(names.size(), types.size());

        for (int i = 0; i < fieldsCount; i++) {
            properties.validateString(SCHEMA + "." + i + "." + SCHEMA_NAME, false, 1);
            properties.validateType(SCHEMA + "." + i + "." + SCHEMA_TYPE, false, false);
            properties.validateString(SCHEMA + "." + i + "." + SCHEMA_FROM, true, 1);
            // either proctime or rowtime
            String proctime = SCHEMA + "." + i + "." + SCHEMA_PROCTIME;
            String rowtime = SCHEMA + "." + i + "." + ROWTIME;

            if (properties.containsKey(proctime)) {
                if (!isStreamEnvironment) {
                    throw new ValidationException(
                            "Property $proctime is not allowed in a batch environment.");
                } else if (proctimeFound) {
                    throw new ValidationException("A proctime attribute must only be defined once.");
                }
                // check proctime
                properties.validateBoolean(proctime, false);
                proctimeFound = properties.getBoolean(proctime);
                // no rowtime
                properties.validatePrefixExclusion(rowtime);
            } else if (properties.hasPrefix(rowtime)) {
                // check rowtime
                RowtimeValidator rowtimeValidator =
                        new RowtimeValidator(
                                supportsSourceTimestamps, supportsSourceWatermarks, SCHEMA + "." + i + ".");
                rowtimeValidator.validate(properties);
                // no proctime
                properties.validateExclusion(proctime);
            }
        }
    }
}
