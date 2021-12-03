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

package org.apache.flink.table.descriptors;

import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.ADMIN_URL_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.SERVICE_URL_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.STARTUP_MODE_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_SINGLE_OPTION_KEY;
import static org.apache.flink.table.descriptors.DescriptorProperties.noValidation;

/**
 * Pulsar Connector validator.
 */
public class PulsarValidator extends ConnectorDescriptorValidator {

    public static final String CONNECTOR_TYPE_VALUE_PULSAR = "pulsar";
    public static final String CONNECTOR_SERVICE_URL = CONNECTOR + "." + SERVICE_URL_OPTION_KEY;
    public static final String CONNECTOR_ADMIN_URL = CONNECTOR + "." + ADMIN_URL_OPTION_KEY;
    public static final String CONNECTOR_TOPIC = CONNECTOR + "." + TOPIC_SINGLE_OPTION_KEY;
    public static final String CONNECTOR_STARTUP_MODE = CONNECTOR + "." + STARTUP_MODE_OPTION_KEY;
    public static final String CONNECTOR_STARTUP_MODE_VALUE_EARLIEST = "earliest";
    public static final String CONNECTOR_STARTUP_MODE_VALUE_LATEST = "latest";
    public static final String CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";
    public static final String CONNECTOR_STARTUP_MODE_VALUE_EXTERNAL_SUB = "external-subscription";
    public static final String CONNECTOR_SPECIFIC_OFFSETS = "connector.specific-offsets";
    public static final String CONNECTOR_SPECIFIC_OFFSETS_PARTITION = "partition";
    public static final String CONNECTOR_SPECIFIC_OFFSETS_OFFSET = "offset";
    public static final String CONNECTOR_EXTERNAL_SUB_NAME = "connector.sub-name";

    public static final String CONNECTOR_PROPERTIES = "connector.properties";
    public static final String CONNECTOR_PROPERTIES_KEY = "key";
    public static final String CONNECTOR_PROPERTIES_VALUE = "value";

    public static final String CONNECTOR_SINK_EXTRACTOR = "connector.sink-extractor";
    public static final String CONNECTOR_SINK_EXTRACTOR_CLASS = "connector.sink-extractor-class";
    public static final String CONNECTOR_SINK_EXTRACTOR_NONE = "none";
    public static final String CONNECTOR_SINK_EXTRACTOR_CUSTOM = "custom";

    @Override
    public void validate(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_TOPIC, false, 1, Integer.MAX_VALUE);
        properties.validateString(CONNECTOR_SERVICE_URL, false, 1, Integer.MAX_VALUE);
        properties.validateString(CONNECTOR_ADMIN_URL, false, 1, Integer.MAX_VALUE);

        validateStartupMode(properties);

        validatePulsarProperties(properties);

        validateSinkExtractor(properties);
    }

    private void validateStartupMode(DescriptorProperties properties) {
        final Map<String, Consumer<String>> specificOffsetValidators = new HashMap<>();
        specificOffsetValidators.put(
                CONNECTOR_SPECIFIC_OFFSETS_PARTITION,
                (key) -> properties.validateString(
                        key,
                        false,
                        1));
        specificOffsetValidators.put(
                CONNECTOR_SPECIFIC_OFFSETS_OFFSET,
                noValidation());

        final Map<String, Consumer<String>> startupModeValidation = new HashMap<>();
        startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_EARLIEST, noValidation());
        startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_LATEST, noValidation());
        startupModeValidation.put(
                CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
                key -> properties.validateFixedIndexedProperties(CONNECTOR_SPECIFIC_OFFSETS, false, specificOffsetValidators));
        properties.validateEnum(CONNECTOR_STARTUP_MODE, true, startupModeValidation);
    }

    private void validatePulsarProperties(DescriptorProperties properties) {
        Map<String, Consumer<String>> propertyValidators = new HashMap<>();

        propertyValidators.put(
                CONNECTOR_PROPERTIES_KEY,
                k -> properties.validateString(k, false, 1));

        propertyValidators.put(
                CONNECTOR_PROPERTIES_VALUE,
                k -> properties.validateString(k, false, 0));

        properties.validateFixedIndexedProperties(CONNECTOR_PROPERTIES, true, propertyValidators);
    }

    private void validateSinkExtractor(DescriptorProperties properties) {
        Map<String, Consumer<String>> sinkValidators = new HashMap<String, Consumer<String>>();

        sinkValidators.put(CONNECTOR_SINK_EXTRACTOR_NONE, noValidation());
        sinkValidators.put(
                CONNECTOR_SINK_EXTRACTOR_CUSTOM,
                k -> properties.validateString(CONNECTOR_SINK_EXTRACTOR_CLASS, false, 1));

        properties.validateEnum(CONNECTOR_SINK_EXTRACTOR, true, sinkValidators);
    }

    public static String normalizeStartupMode(StartupMode startupMode) {
        switch (startupMode) {
            case EARLIEST:
                return CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;
            case LATEST:
                return CONNECTOR_STARTUP_MODE_VALUE_LATEST;
            case SPECIFIC_OFFSETS:
                return CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS;
            case EXTERNAL_SUBSCRIPTION:
                return CONNECTOR_STARTUP_MODE_VALUE_EXTERNAL_SUB;
        }
        throw new IllegalArgumentException("Invalid startup mode.");
    }
}
