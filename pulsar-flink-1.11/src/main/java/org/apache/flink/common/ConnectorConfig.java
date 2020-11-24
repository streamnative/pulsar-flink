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

package org.apache.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.shade.org.apache.bookkeeper.stats.NullStatsProvider;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the connector.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConnectorConfig implements Serializable {

    // ------------------------------------------------------------------------
    //  connection configurations
    // ------------------------------------------------------------------------

    private String serviceUrl;

    private String adminUrl;

    private String zookeeperUri;

    private String authPluginClassName;

    private String authParams;

    private String tlsTrustCertsFilePath;

    private Boolean tlsAllowInsecureConnection;

    private Boolean tlsHostnameVerificationEnable;

    // ------------------------------------------------------------------------
    //  common configurations
    // ------------------------------------------------------------------------

    private String topic;

    private String topics;

    private String topicsPattern;

    // ------------------------------------------------------------------------
    //  segment reader configurations
    // ------------------------------------------------------------------------

    private int entryReadBatchSize = 100;
    private int targetNumSplits = 2;
    private int maxSplitMessageQueueSize = 10000;
    private int maxSplitEntryQueueSize = 1000;
    private int maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE;
    private String statsProvider = NullStatsProvider.class.getName();

    private Map<String, String> statsProviderConfigs = new HashMap<>();

    private boolean namespaceDelimiterRewriteEnable = false;
    private String rewriteNamespaceDelimiter = "/";

    // --- Ledger Offloading ---
    private String managedLedgerOffloadDriver = null;
    private int managedLedgerOffloadMaxThreads = 2;
    private String offloadersDirectory = "./offloaders";
    private Map<String, String> offloaderProperties = new HashMap<>();

    // --- Bookkeeper
    private int bookkeeperThrottleValue = 0;
    private int bookkeeperNumIOThreads = 2 * Runtime.getRuntime().availableProcessors();
    private int bookkeeperNumWorkerThreads = Runtime.getRuntime().availableProcessors();

    // --- ManagedLedger
    private long managedLedgerCacheSizeMB = 0L;
    private int managedLedgerNumWorkerThreads = Runtime.getRuntime().availableProcessors();
    private int managedLedgerNumSchedulerThreads = Runtime.getRuntime().availableProcessors();

    // ------------------------------------------------------------------------
    //  stream reader configurations
    // ------------------------------------------------------------------------

    private long partitionDiscoveryIntervalMillis = -1;

    private boolean failOnDataLoss = true;

    private int clientCacheSize = 5;

    private boolean flushOnCheckpoint = true;

    private boolean failOnWrite = false;

    private int pollTimeoutMs = 120000;

    private int commitMaxRetries = 3;

    public ConnectorConfig(String adminUrl) {
        this.adminUrl = adminUrl;
    }
}
