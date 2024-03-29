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

package org.apache.flink.streaming.connectors.pulsar.table.descriptors;

import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.api.MessageId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_ADMIN_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_EXTERNAL_SUB_NAME;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_PROPERTIES_VALUE;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_SERVICE_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_TOPIC;
import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.PulsarValidator.CONNECTOR_TYPE_VALUE_PULSAR;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

/** Pulsar {@ConnectorDescriptor}. */
public class Pulsar extends ConnectorDescriptor {

    private String topic;

    private String serviceUrl;

    private String adminUrl;

    private StartupMode startupMode;

    private boolean useExtendField;

    private Map<String, MessageId> specificOffsets;

    private String externalSubscriptionName;

    private Map<String, String> pulsarProperties;

    private String sinkExtractorType;

    private String subscriptionPosition;

    public Pulsar() {
        super(CONNECTOR_TYPE_VALUE_PULSAR, 1, true);
    }

    public Pulsar urls(String serviceUrl, String adminUrl) {
        Preconditions.checkNotNull(serviceUrl);
        Preconditions.checkNotNull(adminUrl);
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
        return this;
    }

    /**
     * Sets the topic from which the table is read.
     *
     * @param topic The topic from which the table is read.
     */
    public Pulsar topic(String topic) {
        Preconditions.checkNotNull(topic);
        this.topic = topic;
        return this;
    }

    /**
     * Sets the configuration properties for the Pulsar consumer. Resets previously set properties.
     *
     * @param properties The configuration properties for the Pulsar consumer.
     */
    public Pulsar properties(Properties properties) {
        Preconditions.checkNotNull(properties);
        if (this.pulsarProperties == null) {
            this.pulsarProperties = new HashMap<>();
        }
        this.pulsarProperties.clear();
        properties.forEach(
                (k, v) -> this.pulsarProperties.put(String.valueOf(k), String.valueOf(v)));
        return this;
    }

    public Pulsar useExtendField(boolean useExtendField) {
        this.useExtendField = useExtendField;
        return this;
    }

    /**
     * Adds a configuration properties for the Pulsar consumer.
     *
     * @param key property key for the Pulsar consumer
     * @param value property value for the Pulsar consumer
     */
    public Pulsar property(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        if (this.pulsarProperties == null) {
            this.pulsarProperties = new HashMap<>();
        }
        pulsarProperties.put(key, value);
        return this;
    }

    public Pulsar startFromEarliest() {
        this.startupMode = StartupMode.EARLIEST;
        this.specificOffsets = null;
        return this;
    }

    public Pulsar startFromLatest() {
        this.startupMode = StartupMode.LATEST;
        this.specificOffsets = null;
        return this;
    }

    public Pulsar startFromSpecificOffsets(Map<String, MessageId> specificOffsets) {
        this.startupMode = StartupMode.SPECIFIC_OFFSETS;
        this.specificOffsets = Preconditions.checkNotNull(specificOffsets);
        return this;
    }

    public Pulsar startFromSpecificOffset(String partition, MessageId specificOffset) {
        this.startupMode = StartupMode.SPECIFIC_OFFSETS;
        if (this.specificOffsets == null) {
            this.specificOffsets = new HashMap<>();
        }
        this.specificOffsets.put(partition, specificOffset);
        return this;
    }

    public Pulsar startFromExternalSubscription(String externalSubscriptionName) {
        this.startupMode = StartupMode.EXTERNAL_SUBSCRIPTION;
        this.externalSubscriptionName = externalSubscriptionName;
        return this;
    }

    public Pulsar startFromExternalSubscription(
            String externalSubscriptionName, String subscriptionPosition) {
        this.startupMode = StartupMode.EXTERNAL_SUBSCRIPTION;
        this.subscriptionPosition = subscriptionPosition;
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        if (serviceUrl != null) {
            properties.putString(CONNECTOR_SERVICE_URL, serviceUrl);
        }

        if (adminUrl != null) {
            properties.putString(CONNECTOR_ADMIN_URL, adminUrl);
        }

        if (topic != null) {
            properties.putString(CONNECTOR_TOPIC, topic);
        }

        if (startupMode != null) {
            properties.putString(
                    CONNECTOR_STARTUP_MODE, PulsarValidator.normalizeStartupMode(startupMode));
        }

        if (externalSubscriptionName != null) {
            properties.putString(CONNECTOR_EXTERNAL_SUB_NAME, externalSubscriptionName);
        }

        if (subscriptionPosition != null) {
            properties.putString(CONNECTOR_EXTERNAL_SUB_DEFAULT_OFFSET, subscriptionPosition);
        }

        if (specificOffsets != null) {
            final List<List<String>> values = new ArrayList<>();
            for (Map.Entry<String, MessageId> entry : specificOffsets.entrySet()) {
                values.add(
                        Arrays.asList(entry.getKey(), new String(entry.getValue().toByteArray())));
            }
            properties.putIndexedFixedProperties(
                    CONNECTOR_SPECIFIC_OFFSETS,
                    Arrays.asList(
                            CONNECTOR_SPECIFIC_OFFSETS_PARTITION,
                            CONNECTOR_SPECIFIC_OFFSETS_OFFSET),
                    values);
        }

        if (pulsarProperties != null) {
            properties.putIndexedFixedProperties(
                    CONNECTOR_PROPERTIES,
                    Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE),
                    this.pulsarProperties.entrySet().stream()
                            .map(e -> Arrays.asList(e.getKey(), e.getValue()))
                            .collect(Collectors.toList()));
        }

        properties.putBoolean(
                CONNECTOR + "." + PulsarOptions.USE_EXTEND_FIELD, this.useExtendField);

        return properties.asMap();
    }
}
