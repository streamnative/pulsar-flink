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

package org.apache.flink.table.descriptors;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * test for pulsar connector descriptor.
 */
public class PulsarTest extends DescriptorTestBase {
    private static final String adminUrl = "http://localhost:8080";
    private static final String serviceUrl = "pulsar://localhost:6650";
    private static final MessageId messageId1 = new MessageIdImpl(1, 2, 0);
    private static final MessageId messageId2 = new MessageIdImpl(1, 4, 0);
    private static final MessageId messageId3 = new MessageIdImpl(2, 5, 1);

    @Override
    public List<Descriptor> descriptors() {
        final Descriptor earliestDesc =
                new Pulsar()
                        .urls(serviceUrl, adminUrl)
                        .topic("WhateverTopic")
                        .useExtendField(true)
                        .startFromEarliest();

        final Descriptor specificOffsetsDesc =
                new Pulsar()
                        .urls(serviceUrl, adminUrl)
                        .topic("MyTable")
                        .useExtendField(true)
                        .startFromSpecificOffset("0", messageId1);

        final Map<String, MessageId> offsets = new HashMap<>();
        offsets.put("0", messageId2);
        offsets.put("1", messageId3);

        final Properties properties = new Properties();

        final Descriptor specificOffsetsMapDesc =
                new Pulsar()
                        .urls(serviceUrl, adminUrl)
                        .topic("MyTable")
                        .useExtendField(true)
                        .startFromSpecificOffsets(offsets)
                        .properties(properties);

        final Descriptor startFromLatest =
                new Pulsar()
                        .urls(serviceUrl, adminUrl)
                        .useExtendField(false)
                        .topic("MyTable")
                        .startFromLatest();

        return Arrays.asList(earliestDesc, specificOffsetsDesc, specificOffsetsMapDesc, startFromLatest);
    }

    @Override
    public List<Map<String, String>> properties() {
        final Map<String, String> props1 = new HashMap<>();
        props1.put("connector.property-version", "1");
        props1.put("connector.type", "pulsar");
        props1.put("connector.topic", "WhateverTopic");
        props1.put("connector.startup-mode", "earliest");
        props1.put("connector.service-url", serviceUrl);
        props1.put("connector.admin-url", adminUrl);
        props1.put("connector.use-extend-field", "true");

        final Map<String, String> props2 = new HashMap<>();
        props2.put("connector.property-version", "1");
        props2.put("connector.type", "pulsar");
        props2.put("connector.topic", "MyTable");
        props2.put("connector.startup-mode", "specific-offsets");
        props2.put("connector.specific-offsets.0.partition", "0");
        props2.put("connector.specific-offsets.0.offset", new String(messageId1.toByteArray()));
        props2.put("connector.service-url", serviceUrl);
        props2.put("connector.admin-url", adminUrl);
        props2.put("connector.use-extend-field", "true");

        final Map<String, String> props3 = new HashMap<>();
        props3.put("connector.property-version", "1");
        props3.put("connector.type", "pulsar");
        props3.put("connector.topic", "MyTable");
        props3.put("connector.startup-mode", "specific-offsets");
        props3.put("connector.specific-offsets.0.partition", "0");
        props3.put("connector.specific-offsets.0.offset", new String(messageId2.toByteArray()));
        props3.put("connector.specific-offsets.1.partition", "1");
        props3.put("connector.specific-offsets.1.offset", new String(messageId3.toByteArray()));
        props3.put("connector.service-url", serviceUrl);
        props3.put("connector.admin-url", adminUrl);
        props3.put("connector.use-extend-field", "true");

        final Map<String, String> props4 = new HashMap<>();
        props4.put("connector.property-version", "1");
        props4.put("connector.type", "pulsar");
        props4.put("connector.topic", "MyTable");
        props4.put("connector.startup-mode", "latest");
        props4.put("connector.service-url", serviceUrl);
        props4.put("connector.admin-url", adminUrl);
        props4.put("connector.use-extend-field", "false");

        return Arrays.asList(props1, props2, props3, props4);
    }

    @Override
    public DescriptorValidator validator() {
        return new PulsarValidator();
    }
}
