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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.pulsar.PulsarCatalog;

import org.apache.pulsar.PulsarVersion;

/**
 * {@link ConfigOption}s for {@link PulsarCatalog}.
 */
@Internal
public final class PulsarCatalogFactoryOptions {

    public static final String IDENTIFIER = "pulsar-catalog";

    public static final ConfigOption<String> CATALOG_TENANT =
            ConfigOptions.key("catalog-tenant")
                .stringType()
                .defaultValue(PulsarCatalog.DEFAULT_TENANT)
                .withDescription("Pulsar tenant used to store all table information");

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .defaultValue(PulsarCatalog.DEFAULT_DB);

    public static final ConfigOption<String> ADMIN_URL =
            ConfigOptions.key("catalog-admin-url")
                .stringType()
                .defaultValue("http://localhost:8080")
                .withDescription("Required pulsar cluster admin url");

    public static final ConfigOption<String> SERVICE_URL =
            ConfigOptions.key("catalog-service-url")
                .stringType()
                .defaultValue("pulsar://localhost:6650")
                .withDescription("Required pulsar cluster service url");

    public static final ConfigOption<String>  AUTH_PLUGIN =
            ConfigOptions.key("catalog-auth-plugin")
                .stringType()
                .noDefaultValue()
                .withDescription("Auth plugin name for accessing pulsar cluster");

    public static final ConfigOption<String> AUTH_PARAMS =
            ConfigOptions.key("catalog-auth-params")
                .stringType()
                .noDefaultValue()
                .withDescription("Auth params for accessing pulsar cluster");

    public static final ConfigOption<Integer> DEFAULT_PARTITIONS =
            ConfigOptions.key("table-default-partitions")
                    .intType()
                    .defaultValue(0);

    public static final ConfigOption<String> PULSAR_VERSION =
            ConfigOptions.key("pulsar-version")
                    .stringType()
                    .defaultValue(PulsarVersion.getVersion());

    private PulsarCatalogFactoryOptions() {
    }
}
