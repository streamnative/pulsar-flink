/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.schema;

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
import org.apache.pulsar.common.schema.SchemaType;

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
public class PulsarSchemaOptions {
	private PulsarSchemaOptions() {}

	// --------------------------------------------------------------------------------------------
	// Pulsar specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = ConfigOptions
			.key("fail-on-missing-field")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to specify whether to fail if a field is missing or not, false by default");

	public static final ConfigOption<SchemaType> PULSAR_SCHEMA = ConfigOptions
			.key("pulsar-schema")
			.enumType(SchemaType.class)
			.defaultValue(SchemaType.AVRO)
			.withDescription("Required pulsar schema type enum");

	public static final ConfigOption<String> JSON_OPTIONS = ConfigOptions
			.key("json-options")
			.stringType()
			.noDefaultValue()
			.withDescription("Required pulsar admin connection string");
}
