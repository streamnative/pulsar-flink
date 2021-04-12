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

package org.apache.flink.streaming.connectors.pulsar.internal;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.concurrent.ConcurrentMap;

/**
 * Enable the sharing of same PulsarClient among tasks in a same process.
 */
@Slf4j
@UtilityClass
public class CachedPulsarClient {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static int cacheSize = 100;

    public static void setCacheSize(int newSize) {
        cacheSize = newSize;
    }

    public static int getCacheSize() {
        return cacheSize;
    }

    private static final RemovalListener<String, PulsarClientImpl> removalListener = notification -> {
        String config = notification.getKey();
        PulsarClientImpl client = notification.getValue();
        log.debug("Evicting pulsar client {} with config {}, due to {}", client, config, notification.getCause());

        close(config, client);
    };

    private static final Cache<String, PulsarClientImpl> clientCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .removalListener(removalListener)
        .build();

    private static PulsarClientImpl createPulsarClient(ClientConfigurationData clientConfig) throws PulsarClientException {
        PulsarClientImpl client;
        try {
            client = new PulsarClientImpl(clientConfig);
            log.debug("Created a new instance of PulsarClientImpl for clientConf = {}", clientConfig);
        } catch (PulsarClientException e) {
            log.error("Failed to create PulsarClientImpl for clientConf = {}", clientConfig);
            throw e;
        }
        return client;
    }

    public static synchronized PulsarClientImpl getOrCreate(ClientConfigurationData config) throws PulsarClientException {
        String key = serializeKey(config);
        PulsarClientImpl client = clientCache.getIfPresent(key);

        if (client == null) {
            client = createPulsarClient(config);
        }

        clientCache.put(key, client);

        return client;
    }

    private static void close(String clientConfig, PulsarClientImpl client) {
        if (client != null) {
            try {
                log.info("Closing the Pulsar client with config {}", clientConfig);
                client.close();
            } catch (PulsarClientException e) {
                log.warn(String.format("Error while closing the Pulsar client %s", clientConfig), e);
            }
        }
    }

    @SneakyThrows
    private String serializeKey(ClientConfigurationData clientConfig) {
        return mapper.writeValueAsString(clientConfig);
    }

    @VisibleForTesting
    static void close(ClientConfigurationData clientConfig) {
        String key = serializeKey(clientConfig);
        clientCache.invalidate(key);
    }

    @VisibleForTesting
    static void clear() {
        log.info("Cleaning up guava cache.");
        clientCache.invalidateAll();
    }

    @VisibleForTesting
    static ConcurrentMap<String, PulsarClientImpl> getAsMap() {
        return clientCache.asMap();
    }
}
