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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Unit test of PulsarMetadataReader. */
public class PulsarMetadataReaderTest extends PulsarTestBase {

    private PulsarMetadataReader pulsarMetadataReader;

    private String nonPersistTopic =
            TopicName.get("non-persistent", "public", "default", "NON-PERSIST-TOPIC").toString();
    private String nonPartitionTopic = TopicName.get("NON-P-TOPIC").toString();
    private String onePartitionTopic = TopicName.get("ONE-P-TOPIC").toString();

    @Before
    public void init() throws PulsarClientException {
        Map<String, String> caseInsensitiveParams = new HashMap<>();
        caseInsensitiveParams.put(
                PulsarOptions.TOPIC_MULTI_OPTION_KEY, nonPartitionTopic + "," + onePartitionTopic);
        pulsarMetadataReader =
                new PulsarMetadataReader(
                        adminUrl,
                        clientConfigurationData,
                        "subscribeName",
                        caseInsensitiveParams,
                        0,
                        0);
    }

    @Test
    public void getTopicPartitionsAll() throws PulsarAdminException {

        createNonPartitionTopic(nonPartitionTopic);

        Set<TopicRange> topicPartitionsAll = pulsarMetadataReader.getTopicPartitionsAll();
        List<TopicRange> topicRanges = topicPartitionsAll.stream().collect(Collectors.toList());
        for (TopicRange topicRange : topicRanges) {
            if (topicRange.getTopic().contains(nonPartitionTopic)) {
                assertEquals(topicRange.getTopic(), nonPartitionTopic);
            } else {
                assertEquals(
                        topicRange.getTopic(),
                        onePartitionTopic + PulsarOptions.PARTITION_SUFFIX + 0);
            }
        }

        assertFalse(pulsarMetadataReader.topicExists(nonPartitionTopic));
        assertTrue(pulsarMetadataReader.topicExists(onePartitionTopic));
    }

    @Test
    public void topicExists() throws PulsarAdminException {
        assertFalse(pulsarMetadataReader.topicExists(nonPartitionTopic));

        // non-partitioned topic it doesn't exist
        createNonPartitionTopic(nonPartitionTopic);
        assertFalse(pulsarMetadataReader.topicExists(nonPartitionTopic));

        // non-persist topic exist
        getPulsarAdmin().topics().createPartitionedTopic(nonPersistTopic, 1);
        assertTrue(pulsarMetadataReader.topicExists(nonPersistTopic));
    }

    @After
    public void clearTopic() throws PulsarAdminException {

        pulsarMetadataReader.deleteTopic(nonPartitionTopic);
        pulsarMetadataReader.deleteTopic(onePartitionTopic);
        pulsarMetadataReader.deleteTopic(nonPersistTopic);
    }

    private void createNonPartitionTopic(String topicName) throws PulsarAdminException {
        getPulsarAdmin().topics().createNonPartitionedTopic(topicName);
    }
}
