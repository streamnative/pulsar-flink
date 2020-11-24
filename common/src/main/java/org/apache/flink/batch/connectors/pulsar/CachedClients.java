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

package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.common.ConnectorConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableMap;
import org.apache.pulsar.shade.org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.shade.org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.offload.OffloaderUtils;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.offload.Offloaders;
import org.apache.pulsar.shade.org.apache.bookkeeper.stats.StatsProvider;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.batch.connectors.pulsar.ConnectorUtils.createInstance;
import static org.apache.flink.batch.connectors.pulsar.ConnectorUtils.getProperties;
import static org.apache.pulsar.shade.com.google.common.base.Preconditions.checkNotNull;

/**
 * Cached ML / BK client to be shared among threads inside a process.
 */
@Slf4j
public class CachedClients {

    private static final String OFFLOADERS_DIRECTOR = "offloadersDirectory";
    private static final String MANAGED_LEDGER_OFFLOAD_DRIVER = "managedLedgerOffloadDriver";
    private static final String MANAGED_LEDGER_OFFLOAD_MAX_THREADS = "managedLedgerOffloadMaxThreads";
    static CachedClients instance;
    private final ManagedLedgerFactoryImpl managedLedgerFactory;
    private final StatsProvider statsProvider;
    private OrderedScheduler offloaderScheduler;
    private Offloaders offloaderManager;
    private LedgerOffloader offloader;

    private CachedClients(ConnectorConfig config) throws Exception {
        this.managedLedgerFactory = initManagedLedgerFactory(config);
        this.statsProvider = createInstance(config.getStatsProvider(),
                StatsProvider.class, getClass().getClassLoader());

        // start stats provider
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        config.getStatsProviderConfigs().forEach(clientConfiguration::setProperty);
        this.statsProvider.start(clientConfiguration);

        this.offloader = initManagedLedgerOffloader(config);
    }

    public static CachedClients getInstance(ConnectorConfig config) throws Exception {
        synchronized (CachedClients.class) {
            if (instance == null) {
                instance = new CachedClients(config);
            }
        }
        return instance;
    }

    public static void shutdown() throws Exception {
        synchronized (CachedClients.class) {
            if (instance != null) {
                instance.statsProvider.stop();
                instance.managedLedgerFactory.shutdown();
                instance.offloaderScheduler.shutdown();
                instance.offloaderManager.close();
                instance = null;
            }
        }
    }

    private ManagedLedgerFactoryImpl initManagedLedgerFactory(ConnectorConfig config) throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setZkServers(config.getZookeeperUri())
                .setMetadataServiceUri("zk://" + config.getZookeeperUri() + "/ledgers")
                .setClientTcpNoDelay(false)
                .setUseV2WireProtocol(true)
                .setStickyReadsEnabled(false)
                .setAllowShadedLedgerManagerFactoryClass(true)
                .setShadedLedgerManagerFactoryClassPrefix("org.apache.pulsar.shade.")
                .setReadEntryTimeout(60)
                .setThrottleValue(config.getBookkeeperThrottleValue())
                .setNumIOThreads(config.getBookkeeperNumIOThreads())
                .setNumWorkerThreads(config.getBookkeeperNumWorkerThreads());

        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(config.getManagedLedgerCacheSizeMB());
        managedLedgerFactoryConfig.setNumManagedLedgerWorkerThreads(
                config.getManagedLedgerNumWorkerThreads());
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(
                config.getManagedLedgerNumSchedulerThreads());

        return new ManagedLedgerFactoryImpl(bkClientConfiguration, managedLedgerFactoryConfig);
    }

    private synchronized OrderedScheduler getOffloaderScheduler(ConnectorConfig config) {
        if (this.offloaderScheduler == null) {
            this.offloaderScheduler = OrderedScheduler.newSchedulerBuilder()
                    .numThreads(config.getManagedLedgerOffloadMaxThreads())
                    .name("pulsar-offloader").build();
        }
        return this.offloaderScheduler;
    }

    private LedgerOffloader initManagedLedgerOffloader(ConnectorConfig config) {

        try {
            if (StringUtils.isNotBlank(config.getManagedLedgerOffloadDriver())) {
                checkNotNull(config.getOffloadersDirectory(),
                        "Offloader driver is configured to be '%s' but no offloaders directory is configured.",
                        config.getManagedLedgerOffloadDriver());
                this.offloaderManager = OffloaderUtils.searchForOffloaders(config.getOffloadersDirectory(), "");
                LedgerOffloaderFactory offloaderFactory = this.offloaderManager.getOffloaderFactory(
                        config.getManagedLedgerOffloadDriver());

                Map<String, String> offloaderProperties = config.getOffloaderProperties();
                offloaderProperties.put(OFFLOADERS_DIRECTOR, config.getOffloadersDirectory());
                offloaderProperties.put(MANAGED_LEDGER_OFFLOAD_DRIVER, config.getManagedLedgerOffloadDriver());
                offloaderProperties
                        .put(MANAGED_LEDGER_OFFLOAD_MAX_THREADS,
                                String.valueOf(config.getManagedLedgerOffloadMaxThreads()));

                try {
                    return offloaderFactory.create(
                            getProperties(offloaderProperties),
                            ImmutableMap.of(
                                    LedgerOffloader.METADATA_SOFTWARE_VERSION_KEY.toLowerCase(),
                                    PulsarVersion.getVersion(),
                                    LedgerOffloader.METADATA_SOFTWARE_GITSHA_KEY.toLowerCase(),
                                    PulsarVersion.getGitSha()
                            ),
                            getOffloaderScheduler(config));
                } catch (IOException ioe) {
                    log.error("Failed to create offloader: ", ioe);
                    throw new RuntimeException(ioe.getMessage(), ioe.getCause());
                }
            } else {
                log.info("No ledger offloader configured, using NULL instance");
                return NullLedgerOffloader.INSTANCE;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public ManagedLedgerConfig getManagedLedgerConfig() {

        return new ManagedLedgerConfig()
                .setLedgerOffloader(this.offloader);
    }

    public ManagedLedgerFactoryImpl getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    public StatsProvider getStatsProvider() {
        return statsProvider;
    }
}
