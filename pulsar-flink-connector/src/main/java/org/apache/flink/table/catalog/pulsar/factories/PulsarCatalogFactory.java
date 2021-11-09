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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.ADMIN_URL;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.AUTH_PARAMS;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.AUTH_PLUGIN;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.DEFAULT_PARTITIONS;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.CATALOG_TENANT;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.IDENTIFIER;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.PULSAR_VERSION;
import static org.apache.flink.table.catalog.pulsar.factories.PulsarCatalogFactoryOptions.SERVICE_URL;

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
                context.getName(),
                helper.getOptions().get(ADMIN_URL),
                helper.getOptions().get(SERVICE_URL),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(CATALOG_TENANT),
                helper.getOptions().get(AUTH_PLUGIN),
                helper.getOptions().get(AUTH_PARAMS));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        // pulsar catalog options
        options.add(ADMIN_URL);
        options.add(SERVICE_URL);
        options.add(CATALOG_TENANT);
        options.add(DEFAULT_DATABASE);
        options.add(AUTH_PLUGIN);
        options.add(AUTH_PARAMS);
        options.add(DEFAULT_PARTITIONS);
        options.add(PULSAR_VERSION);

        // TODO(nlu): investigate if need to provide default table options

        return options;
    }
}
