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

package org.apache.flink.table.catalog.pulsar.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.pulsar.PulsarCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PROPERTIES;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SERVICE_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SINK_SEMANTIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.VALUE_FORMAT;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.DEFAULT_PARTITIONS;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.IDENTIFIER;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.PULSAR_VERSION;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/**
 * Pulsar {@CatalogFactory}.
 */
public class PulsarCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        return new PulsarCatalog(
                helper.getOptions().get(ADMIN_URL),
                context.getName(),
                context.getOptions(),
                helper.getOptions().get(DEFAULT_DATABASE));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ADMIN_URL);
        options.add(SERVICE_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> props = new HashSet<>();
        props.add(DEFAULT_DATABASE);
        props.add(PROPERTY_VERSION);
        props.add(SCAN_STARTUP_MODE);
        props.add(DEFAULT_PARTITIONS);
        props.add(KEY_FORMAT);
        props.add(KEY_FIELDS);
        props.add(KEY_FIELDS_PREFIX);
        props.add(VALUE_FORMAT);
        props.add(VALUE_FIELDS_INCLUDE);
        props.add(SINK_SEMANTIC);
        props.add(PULSAR_VERSION);
        props.add(FactoryUtil.FORMAT);
        props.add(PROPERTIES);
        return props;
    }
}
