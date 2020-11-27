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

package org.apache.flink.batch.connectors.pulsar.common;

import org.apache.flink.pulsar.common.ConnectorConfig;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.FastThreadLocal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Utils on loading ConnectorConfig from Map.
 */
public final class ConnectorConfigUtils {

    private static final FastThreadLocal<ObjectMapper> mapper = new FastThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() throws Exception {
            return create();
        }
    };

    private ConnectorConfigUtils() {
    }

    public static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }

    public static ObjectMapper getThreadLocal() {
        return mapper.get();
    }

    public static org.apache.flink.pulsar.common.ConnectorConfig loadData(Map<String, String> config) {
        return loadData(config, new org.apache.flink.pulsar.common.ConnectorConfig());
    }

    public static org.apache.flink.pulsar.common.ConnectorConfig loadData(Map<String, String> config,
                                           org.apache.flink.pulsar.common.ConnectorConfig existingData) {
        ObjectMapper mapper = getThreadLocal();
        try {
            String existingConfigJson = mapper.writeValueAsString(existingData);
            Map<String, Object> existingConfig = mapper.readValue(existingConfigJson, Map.class);
            Map<String, Object> newConfig = Maps.newHashMap();
            newConfig.putAll(existingConfig);
            newConfig.putAll(config);
            String configJson = mapper.writeValueAsString(newConfig);
            return mapper.readValue(configJson, ConnectorConfig.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config into existing configuration data", e);
        }
    }
}
