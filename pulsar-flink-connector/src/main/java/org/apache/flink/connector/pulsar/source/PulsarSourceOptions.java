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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configurations for PulsarSource.
 */
public class PulsarSourceOptions {

    public static final ConfigOption<String> ADMIN_URL = ConfigOptions
            .key("admin.url")
            .stringType()
            .noDefaultValue()
            .withDescription("The url to Pulsar admin.");

    public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MS = ConfigOptions
            .key("partition.discovery.interval.ms")
            .longType()
            .defaultValue(30000L)
            .withDescription("The interval in milliseconds for the Pulsar source to discover " +
                    "the new partitions. A non-positive value disables the partition discovery.");

    public static final ConfigOption<Long> CLOSE_TIMEOUT_MS = ConfigOptions
            .key("close.timeout.ms")
            .longType()
            .defaultValue(10000L)
            .withDescription("The max time to wait when closing components.");

    public static final ConfigOption<Long> MAX_FETCH_TIME = ConfigOptions
            .key("max.fetch.time")
            .longType()
            .defaultValue(500L)
            .withDescription("The max time to wait when fetching records. " +
                    "A longer time increases throughput but also latency. " +
                    "A fetch batch might be finished earlier because of max.fetch.records.");

    public static final ConfigOption<Integer> MAX_FETCH_RECORDS = ConfigOptions
            .key("max.fetch.records")
            .intType()
            .defaultValue(100)
            .withDescription("The max number of records to fetch to wait when polling. " +
                    "A longer time increases throughput but also latency." +
                    "A fetch batch might be finished earlier because of max.fetch.time.");

    public static final ConfigOption<OffsetVerification> VERIFY_INITIAL_OFFSETS = ConfigOptions
            .key("verify.initial.offsets")
            .enumType(OffsetVerification.class)
            .defaultValue(OffsetVerification.FAIL_ON_MISMATCH)
            .withDescription("Upon (re)starting the source checks whether the expected message can be read. " +
                    "If failure is enabled the application fails, else it logs a warning. " +
                    "A possible solution is to adjust the retention settings in pulsar or ignoring the check result.");

    /**
     * Enum for offsetVerification.
     */
    public enum OffsetVerification {
        FAIL_ON_MISMATCH, WARN_ON_MISMATCH, IGNORE
    }
}
