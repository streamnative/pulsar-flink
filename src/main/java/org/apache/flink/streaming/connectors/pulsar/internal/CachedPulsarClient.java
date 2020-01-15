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
package org.apache.flink.streaming.connectors.pulsar.internal;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.com.google.common.cache.CacheBuilder;
import org.apache.pulsar.shade.com.google.common.cache.CacheLoader;
import org.apache.pulsar.shade.com.google.common.cache.LoadingCache;
import org.apache.pulsar.shade.com.google.common.cache.RemovalListener;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

@Slf4j
public class CachedPulsarClient {
    
    private static int cacheSize = 5;

    public static void setCacheSize(int size) {
        cacheSize = size;
    }

    public static int getCacheSize() {
        return cacheSize;
    }

    private static CacheLoader<ClientConfigurationData, PulsarClientImpl> cacheLoader =
        new CacheLoader<ClientConfigurationData, PulsarClientImpl>() {
            @Override
            public PulsarClientImpl load(ClientConfigurationData key) throws Exception {
                return createPulsarClient(key);
            }
        };

    private static RemovalListener<ClientConfigurationData, PulsarClientImpl> removalListener = notification -> {
        ClientConfigurationData config = notification.getKey();
        PulsarClientImpl client = notification.getValue();
        log.debug("Evicting pulsar client %s with config %s, due to %s",
            client.toString(), config.toString(), notification.getCause().toString());
        close(config, client);
    };

    private static LoadingCache<ClientConfigurationData, PulsarClientImpl> guavaCache =
        CacheBuilder.newBuilder().maximumSize(cacheSize).removalListener(removalListener).build(cacheLoader);

    private static PulsarClientImpl createPulsarClient(
        ClientConfigurationData clientConfig) throws PulsarClientException {
        PulsarClientImpl client;
        try {
            client = new PulsarClientImpl(clientConfig);
            log.debug(String.format("Created a new instance of PulsarClientImpl for clientConf = %s",
                clientConfig.toString()));
        } catch (PulsarClientException e) {
            log.error(String.format("Failed to create PulsarClientImpl for clientConf = %s",
                clientConfig.toString()));
            throw e;
        }
        return client;
    }

    public static PulsarClientImpl getOrCreate(ClientConfigurationData config) throws ExecutionException {
        return guavaCache.get(config);
    }

    private static void close(ClientConfigurationData clientConfig, PulsarClientImpl client) {
        try {
            log.info(String.format("Closing the Pulsar client with conifg %s", clientConfig.toString()));
            client.close();
        } catch (PulsarClientException e) {
            log.warn(String.format("Error while closing the Pulsar client ", clientConfig.toString()), e);
        }
    }

    static void close(ClientConfigurationData clientConfig) {
        guavaCache.invalidate(clientConfig);
    }

    static void clear() {
        log.info("Cleaning up guava cache.");
        guavaCache.invalidateAll();
    }

    static ConcurrentMap<ClientConfigurationData, PulsarClientImpl> getAsMap() {
        return guavaCache.asMap();
    }
}
