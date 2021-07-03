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

import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PulsarMetadataReaderTest extends PulsarTestBase {

    private PulsarMetadataReader pulsarMetadataReader;

    private String partitionEq0Topic = "PA0_TOPIC";
    private String partitionEq1Topic = "PA1_TOPIC";

    @Before
    public void init() throws PulsarClientException {
        Map<String, String> caseInsensitiveParams = new HashMap<>();
        caseInsensitiveParams.put(PulsarOptions.TOPIC_MULTI_OPTION_KEY, partitionEq0Topic + "," + partitionEq1Topic);
        pulsarMetadataReader = new PulsarMetadataReader(adminUrl, clientConfigurationData, "subscribeName", caseInsensitiveParams, 0, 0);
    }

    @Test
    public void getTopicPartitionsAll() throws PulsarAdminException, PulsarClientException {

        createPartitionsEq0Topic(partitionEq0Topic);

        Set<TopicRange> topicPartitionsAll = pulsarMetadataReader.getTopicPartitionsAll();
        List<TopicRange> topicRanges = topicPartitionsAll.stream().collect(Collectors.toList());
        for (TopicRange topicRange : topicRanges) {
            if (topicRange.getTopic().contains(partitionEq0Topic)) {
                Assert.assertEquals(topicRange.getTopic(), TopicName.get(partitionEq0Topic).toString());
            } else {
                Assert.assertEquals(topicRange.getTopic(), TopicName.get(partitionEq1Topic) + PulsarOptions.PARTITION_SUFFIX + 0);
            }
        }

        Assert.assertFalse(pulsarMetadataReader.topicExists(TopicName.get(partitionEq0Topic).toString()));
        Assert.assertTrue(pulsarMetadataReader.topicExists(TopicName.get(partitionEq1Topic).toString()));

        pulsarMetadataReader.deleteTopic(TopicName.get(partitionEq0Topic).toString());
        pulsarMetadataReader.deleteTopic(TopicName.get(partitionEq1Topic).toString());
    }

    @Test
    public void topicExists() throws PulsarAdminException, PulsarClientException {
        String topicName = "not-exit-topic";
        Assert.assertFalse(pulsarMetadataReader.topicExists(topicName));

        // partitions == 0 it doesn't exist
        createPartitionsEq0Topic(topicName);
        Assert.assertFalse(pulsarMetadataReader.topicExists(topicName));

        pulsarMetadataReader.deleteTopic(topicName);
    }

    private void createPartitionsEq0Topic(String topicName) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
        client.newProducer(Schema.STRING).topic(topicName).create();
    }

}
