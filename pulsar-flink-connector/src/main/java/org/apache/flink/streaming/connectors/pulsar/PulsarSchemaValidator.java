/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;

/**
 * Pulsar validator that allows to only specify proctime or rowtime in schema. Other fields could be
 * inferred from Pulsar schema.
 */
public class PulsarSchemaValidator extends SchemaValidator {

    public PulsarSchemaValidator(
            boolean isStreamEnvironment,
            boolean supportsSourceTimestamps,
            boolean supportsSourceWatermarks) {
        super(isStreamEnvironment, supportsSourceTimestamps, supportsSourceWatermarks);
    }

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
    }
}
