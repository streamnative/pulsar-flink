package org.apache.flink.table.catalog.pulsar.descriptors;

import lombok.val;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class PulsarCatalogValidator extends CatalogDescriptorValidator {

    public static final String CATALOG_TYPE_VALUE_PULSAR = "pulsar";
    public static final String CATALOG_PULSAR_VERSION = "pulsar-version";
    public static final String CATALOG_SERVICE_URL = PulsarOptions.SERVICE_URL_OPTION_KEY;
    public static final String CATALOG_ADMIN_URL = PulsarOptions.ADMIN_URL_OPTION_KEY;
    public static final String CATALOG_STARTING_POS = PulsarOptions.STARTING_OFFSETS_OPTION_KEY;
    public static final String CATALOG_DEFAULT_PARTITIONS = PulsarOptions.DEFAULT_PARTITIONS;

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR, false);
        properties.validateString(CATALOG_PULSAR_VERSION, true, 1);
        properties.validateString(CATALOG_SERVICE_URL, false, 1);
        properties.validateString(CATALOG_ADMIN_URL, false, 1);
        properties.validateInt(CATALOG_DEFAULT_PARTITIONS, true, 1);
        validateStartingOffsets(properties);
    }

    private void validateStartingOffsets(DescriptorProperties properties) {
        if (properties.containsKey(CATALOG_STARTING_POS)) {
            val v = properties.getString(CATALOG_STARTING_POS);
            if (v != "earliest" && v != "latest") {
                throw new ValidationException(CATALOG_STARTING_POS + " should be either earliest or latest");
            }
        }
    }
}
