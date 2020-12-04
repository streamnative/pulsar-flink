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

package org.apache.flink.table.catalog.pulsar;

import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;

/**
 * Pulsar {@CatalogDescriptorValidator}.
 */
public class PulsarCatalogValidator extends CatalogDescriptorValidator {

    public static final String CATALOG_TYPE_VALUE_PULSAR = "pulsar";
    public static final String CATALOG_PULSAR_VERSION = "pulsar-version";
    public static final String CATALOG_SERVICE_URL = PulsarOptions.SERVICE_URL_OPTION_KEY;
    public static final String CATALOG_ADMIN_URL = PulsarOptions.ADMIN_URL_OPTION_KEY;
    public static final String CATALOG_STARTUP_MODE = PulsarOptions.STARTUP_MODE_OPTION_KEY;
    public static final String CATALOG_DEFAULT_PARTITIONS = PulsarOptions.DEFAULT_PARTITIONS;

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR, false);
        properties.validateString(CATALOG_PULSAR_VERSION, true, 1);
        properties.validateString(CATALOG_SERVICE_URL, false, 1);
        properties.validateString(CATALOG_ADMIN_URL, false, 1);
        properties.validateInt(CATALOG_DEFAULT_PARTITIONS, true, 1);
        properties.validateString(FormatDescriptorValidator.FORMAT_TYPE, false);
        validateStartingOffsets(properties);
    }

    private void validateStartingOffsets(DescriptorProperties properties) {
        if (properties.containsKey(CATALOG_STARTUP_MODE)) {
            String v = properties.getString(CATALOG_STARTUP_MODE);
            if (!v.equals("earliest") && !v.equals("latest")) {
                throw new ValidationException(CATALOG_STARTUP_MODE + " should be either earliest or latest");
            }
        }
    }
}
