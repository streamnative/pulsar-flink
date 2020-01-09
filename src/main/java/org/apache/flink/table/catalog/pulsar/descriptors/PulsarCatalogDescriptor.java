package org.apache.flink.table.catalog.pulsar.descriptors;

import lombok.val;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_PULSAR_VERSION;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_TYPE_VALUE_PULSAR;

public class PulsarCatalogDescriptor extends CatalogDescriptor {

    private String pulsarVersion;

    public PulsarCatalogDescriptor() {
        super(CATALOG_TYPE_VALUE_PULSAR, 1, "public/default");
    }

    public PulsarCatalogDescriptor pulsarVersion(String pulsarVersion) {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(pulsarVersion));
        this.pulsarVersion = pulsarVersion;

        return this;
    }

    @Override
    protected Map<String, String> toCatalogProperties() {
        val props = new DescriptorProperties();

        if (pulsarVersion != null) {
            props.putString(CATALOG_PULSAR_VERSION, pulsarVersion);
        }

        return props.asMap();
    }
}
