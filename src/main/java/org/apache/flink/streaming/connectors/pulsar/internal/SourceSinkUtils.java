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

import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class SourceSinkUtils {

    public static Map<String, String> validateStreamSourceOptions(Map<String, String> parameters) {
        val caseInsensitiveParams = parameters.entrySet().stream()
            .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue().toLowerCase(Locale.ROOT)));

        if (caseInsensitiveParams.containsKey(PulsarOptions.ENDING_OFFSETS_OPTION_KEY)) {
            throw new IllegalArgumentException("ending offset not valid in streaming queries");
        }

        return validateSourceOptions(caseInsensitiveParams);
    }

    private static Map<String, String> validateSourceOptions(Map<String, String> caseInsensitiveParams) {
        val topicOptions = caseInsensitiveParams.entrySet().stream()
            .filter(t -> PulsarOptions.TOPIC_OPTION_KEYS.contains(t))
            .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));

        if (topicOptions.isEmpty() || topicOptions.size() > 1) {
            throw new IllegalArgumentException(
                "You should specify topic(s) using one of the topic options: " +
                    StringUtils.join(PulsarOptions.TOPIC_OPTION_KEYS, ","));
        }

        for (Map.Entry<String, String> topicEntry : topicOptions.entrySet()) {
            val key = topicEntry.getKey();
            val value = topicEntry.getValue();
            if (key == "topic") {
                if (value.contains(",")) {
                    throw new IllegalArgumentException(
                        "Use `topics` instead of `topic` for multi topic read");
                }
            } else if (key == "topics") {
                val topics = Arrays.asList(value.split(",")).stream()
                    .map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
                if (topics.isEmpty()) {
                    throw new IllegalArgumentException(
                        "No topics is specified for read with option" + value);
                }
            } else {
                if (value.trim().length() == 0) {
                    throw new IllegalArgumentException("TopicsPattern is empty");
                }
            }
        }
        return caseInsensitiveParams;
    }

    public static boolean belongsTo(String topic, int numParallelSubtasks, int index) {
        return (topic.hashCode() * 31 & Integer.MAX_VALUE) % numParallelSubtasks == index;
    }

    public static long getPartitionDiscoveryIntervalInMillis(Map<String, String> parameters) {
        val interval = parameters.getOrDefault(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY, "-1");
        return Long.parseLong(interval);
    }

    public static int getPollTimeoutMs(Map<String, String> parameters) {
        val interval = parameters.getOrDefault(PulsarOptions.POLL_TIMEOUT_MS_OPTION_KEY, "120000");
        return Integer.parseInt(interval);
    }

    public static int getClientCacheSize(Map<String, String> parameters) {
        val size = parameters.getOrDefault(PulsarOptions.CLIENT_CACHE_SIZE_OPTION_KEY, "5");
        return Integer.parseInt(size);
    }

    public static boolean flushOnCheckpoint(Map<String, String> parameters) {
        val b = parameters.getOrDefault(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
        return Boolean.parseBoolean(b);
    }

    public static boolean failOnWrite(Map<String, String> parameters) {
        val b = parameters.getOrDefault(PulsarOptions.FAIL_ON_WRITE_OPTION_KEY, "false");
        return Boolean.parseBoolean(b);
    }

    public static Map<String, Object> getReaderParams(Map<String, String> parameters) {
        return parameters.keySet().stream()
            .filter(k -> k.startsWith(PulsarOptions.PULSAR_READER_OPTION_KEY_PREFIX))
            .collect(Collectors.toMap(k -> k.substring(PulsarOptions.PULSAR_READER_OPTION_KEY_PREFIX.length()), k -> parameters.get(k)));
    }

    public static Map<String, String> toCaceInsensitiveParams(Map<String, String> parameters) {
        return parameters.entrySet().stream()
            .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue().toLowerCase(Locale.ROOT)));
    }

    public static Map<String, Object> getProducerParams(Map<String, String> parameters) {
        return parameters.keySet().stream()
            .filter(k -> k.startsWith(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX))
            .collect(Collectors.toMap(k -> k.substring(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX.length()), k -> parameters.get(k)));
    }

}