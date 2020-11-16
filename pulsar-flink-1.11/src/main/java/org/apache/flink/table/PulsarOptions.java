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

package org.apache.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.PulsarValidator;
import org.apache.flink.util.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Option utils for pulsar table source sink.
 */
@Slf4j
public class PulsarOptions {
	private PulsarOptions() {}

	// --------------------------------------------------------------------------------------------
	// Pulsar specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> TOPIC = ConfigOptions
			.key("topic")
			.stringType()
			.noDefaultValue()
			.withDescription("Required topic name from which the table is read");

	public static final ConfigOption<String> SERVICE_URL = ConfigOptions
			.key("service-url")
			.stringType()
			.noDefaultValue()
			.withDescription("Required pulsar server connection string");

	public static final ConfigOption<String> ADMIN_URL = ConfigOptions
			.key("admin-url")
			.stringType()
			.noDefaultValue()
			.withDescription("Required pulsar admin connection string");

	// --------------------------------------------------------------------------------------------
	// Scan specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions
			.key("scan.startup.mode")
			.stringType()
			.defaultValue("latest")
			.withDescription("Optional startup mode for Pulsar consumer, valid enumerations are "
					+ "\"earliest\", \"latest\", \"external-subscription\",\n"
					+ "or \"specific-offsets\"");

	public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS = ConfigOptions
			.key("scan.startup.specific-offsets")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional offsets used in case of \"specific-offsets\" startup mode");

	public static final ConfigOption<String> SCAN_STARTUP_SUB_NAME = ConfigOptions
			.key("scan.startup.sub-name")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional sub-name used in case of \"specific-offsets\" startup mode");

	public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS = ConfigOptions
			.key("scan.startup.timestamp-millis")
			.longType()
			.noDefaultValue()
			.withDescription("Optional timestamp used in case of \"timestamp\" startup mode");
	public static final ConfigOption<String> PULSAR_READER_READER_NAME = ConfigOptions
			.key("pulsar.reader.readerName")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional pulsar reader readerName of \"readerName\"");
	public static final ConfigOption<String> PULSAR_READER_SUBSCRIPTION_ROLE_PREFIX = ConfigOptions
			.key("pulsar.reader.subscriptionRolePrefix")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional pulsar reader subscriptionRolePrefix of \"subscriptionRolePrefix\"");
	public static final ConfigOption<String> PULSAR_READER_RECEIVER_QUEUE_SIZE = ConfigOptions
			.key("pulsar.reader.receiver-queue-size")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional pulsar reader receiverQueueSize of \"receiver-queue-size\"");

	public static final ConfigOption<String> PARTITION_DISCOVERY_INTERVAL_MILLIS = ConfigOptions
			.key("partition.discovery.interval-millis")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional discovery topic interval of \"interval-millis\" millis");

	// --------------------------------------------------------------------------------------------
	// Sink specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> SINK_PARTITIONER = ConfigOptions
			.key("sink.partitioner")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional output partitioning from Flink's partitions\n"
					+ "into pulsar's partitions valid enumerations are\n"
					+ "\"fixed\": (each Flink partition ends up in at most one pulsar partition),\n"
					+ "\"round-robin\": (a Flink partition is distributed to pulsar partitions round-robin)\n"
					+ "\"custom class name\": (use a custom FlinkKafkaPartitioner subclass)");

	// --------------------------------------------------------------------------------------------
	// Option enumerations
	// --------------------------------------------------------------------------------------------

	// Start up offset.
	public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest";
	public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest";
	public static final String SCAN_STARTUP_MODE_VALUE_EXTERNAL_SUBSCRIPTION = "external-subscription";
	public static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";

	private static final Set<String> SCAN_STARTUP_MODE_ENUMS = new HashSet<>(Arrays.asList(
			SCAN_STARTUP_MODE_VALUE_EARLIEST,
			SCAN_STARTUP_MODE_VALUE_LATEST,
			SCAN_STARTUP_MODE_VALUE_EXTERNAL_SUBSCRIPTION,
			SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS));

	// Sink partitioner.
	public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
	public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";

	private static final Set<String> SINK_PARTITIONER_ENUMS = new HashSet<>(Arrays.asList(
			SINK_PARTITIONER_VALUE_FIXED,
			SINK_PARTITIONER_VALUE_ROUND_ROBIN));

	// Prefix for pulsar specific properties.
	public static final String PROPERTIES_PREFIX = "properties.";

	// Other keywords.
	private static final String PARTITION = "partition";
	private static final String OFFSET = "offset";

	// --------------------------------------------------------------------------------------------
	// Validation
	// --------------------------------------------------------------------------------------------

	public static void validateTableOptions(ReadableConfig tableOptions) {
		validateScanStartupMode(tableOptions);
		validateSinkPartitioner(tableOptions);
	}

	private static void validateScanStartupMode(ReadableConfig tableOptions) {
		tableOptions.getOptional(SCAN_STARTUP_MODE)
				.map(String::toLowerCase)
				.ifPresent(mode -> {
					if (!SCAN_STARTUP_MODE_ENUMS.contains(mode)) {
						throw new ValidationException(
								String.format("Invalid value for option '%s'. Supported values are %s, but was: %s",
										SCAN_STARTUP_MODE.key(),
										"[earliest-offset, latest-offset, group-offsets, specific-offsets, timestamp]",
										mode));
					}

					if (mode.equals(SCAN_STARTUP_MODE_VALUE_EXTERNAL_SUBSCRIPTION)) {
						if (!tableOptions.getOptional(SCAN_STARTUP_SUB_NAME).isPresent()) {
							throw new ValidationException(String.format("'%s' is required in '%s' startup mode"
											+ " but missing.",
									SCAN_STARTUP_SUB_NAME.key(),
									SCAN_STARTUP_SUB_NAME));
						}
					}
					if (mode.equals(SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS)) {
						if (!tableOptions.getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS).isPresent()) {
							throw new ValidationException(String.format("'%s' is required in '%s' startup mode"
											+ " but missing.",
									SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
									SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS));
						}
						String specificOffsets = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
						parseSpecificOffsets(specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
			}
		});
	}

	private static void validateSinkPartitioner(ReadableConfig tableOptions) {
		tableOptions.getOptional(SINK_PARTITIONER)
				.ifPresent(partitioner -> {
					if (!SINK_PARTITIONER_ENUMS.contains(partitioner.toLowerCase())) {
						if (partitioner.isEmpty()) {
							throw new ValidationException(
									String.format("Option '%s' should be a non-empty string.",
											SINK_PARTITIONER.key()));
						}
					}
				});
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	public static StartupOptions getStartupOptions(
			ReadableConfig tableOptions,
			String topic) {

		final Map<String, MessageId> specificOffsets = new HashMap<>();
		final List<String> subName = new ArrayList<>(1);
		final StartupMode startupMode = tableOptions.getOptional(SCAN_STARTUP_MODE)
				.map(modeString -> {
					switch (modeString) {
						case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
							return StartupMode.EARLIEST;

						case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
							return StartupMode.LATEST;

						case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
							// TODO
							String specificOffsetsStrOpt = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);

							final Map<Integer, String> offsetList = parseSpecificOffsets(
									specificOffsetsStrOpt,
									SCAN_STARTUP_SPECIFIC_OFFSETS.key());
							offsetList.forEach((partition, offset) -> {
								try {
									specificOffsets.put(partition.toString(), MessageId.fromByteArray(offset.getBytes()));
								} catch (IOException e) {
									log.error("Failed to decode message id from properties {}",
											ExceptionUtils.stringifyException(e));
									throw new RuntimeException(e);
								}
							});
							return StartupMode.SPECIFIC_OFFSETS;

						case PulsarValidator.CONNECTOR_STARTUP_MODE_VALUE_EXTERNAL_SUB:
							subName.add(tableOptions.get(SCAN_STARTUP_SUB_NAME));
							return StartupMode.EXTERNAL_SUBSCRIPTION;

						default:
							throw new TableException("Unsupported startup mode. Validator should have checked that.");
					}
				}).orElse(StartupMode.LATEST);
		final StartupOptions options = new StartupOptions();
		options.startupMode = startupMode;
		options.specificOffsets = specificOffsets;
		if (subName.size() != 0) {
			options.externalSubscriptionName = subName.get(0);
		}
		return options;

	}

	/**
	 * Parses SpecificOffsets String to Map.
	 *
	 * <p>SpecificOffsets String format was given as following:
	 *
	 * <pre>
	 *     scan.startup.specific-offsets = 42:1012:0;44:1011:1
	 * </pre>
	 *
	 * @return SpecificOffsets with Map format, key is partition, and value is offset
	 */
	public static Map<Integer, String> parseSpecificOffsets(
			String specificOffsetsStr,
			String optionKey) {
		final Map<Integer, String> offsetMap = new HashMap<>();
		final String[] pairs = specificOffsetsStr.split(";");
		final String validationExceptionMessage = String.format(
				"Invalid properties '%s' should follow the format "
						+ "messageId with partition'42:1012:0;44:1011:1', but is '%s'.",
				optionKey,
				specificOffsetsStr);

		if (pairs.length == 0) {
			throw new ValidationException(validationExceptionMessage);
		}

		for (String pair : pairs) {
			if (null == pair || pair.length() == 0 || !pair.contains(",")) {
				throw new ValidationException(validationExceptionMessage);
			}

			final String[] kv = pair.split(":");
			if (kv.length != 3) {
				throw new ValidationException(validationExceptionMessage);
			}

			String partitionValue = kv[2];
			String offsetValue = pair;
			try {
				final Integer partition = Integer.valueOf(partitionValue);
				offsetMap.put(partition, offsetValue);
			} catch (NumberFormatException e) {
				throw new ValidationException(validationExceptionMessage, e);
			}
		}
		return offsetMap;
	}

	// --------------------------------------------------------------------------------------------
	// Inner classes
	// --------------------------------------------------------------------------------------------

	/** pulsar startup options. **/
	public static class StartupOptions {
		public StartupMode startupMode;
		public Map<String, MessageId> specificOffsets;
		public String externalSubscriptionName;
	}
}
