package org.apache.flink.table.catalog.pulsar.factories;

import lombok.val;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.pulsar.PulsarCatalog;
import org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_ADMIN_URL;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_DEFAULT_PARTITIONS;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_PULSAR_VERSION;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_SERVICE_URL;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_STARTING_POS;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_TYPE_VALUE_PULSAR;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

public class PulsarCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        val dp = getValidateProperties(properties);
        val defaultDB = dp.getOptionalString(CATALOG_DEFAULT_DATABASE).orElse("public/default");
        val adminUrl = dp.getString(CATALOG_ADMIN_URL);

        return new PulsarCatalog(adminUrl, name, dp.asMap(), defaultDB);
    }

    @Override
    public Map<String, String> requiredContext() {
        val context = new HashMap<String, String>();
        context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR);
        context.put(CATALOG_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        val props = new ArrayList<String>();
        props.add(CATALOG_DEFAULT_DATABASE);
        props.add(CATALOG_PULSAR_VERSION);
        props.add(CATALOG_SERVICE_URL);
        props.add(CATALOG_ADMIN_URL);
        props.add(CATALOG_STARTING_POS);
        props.add(CATALOG_DEFAULT_PARTITIONS);
        return props;
    }

    private DescriptorProperties getValidateProperties(Map<String, String> properties) {
        val dp = new DescriptorProperties();
        dp.putProperties(properties);
        new PulsarCatalogValidator().validate(dp);
        return dp;
    }
}
