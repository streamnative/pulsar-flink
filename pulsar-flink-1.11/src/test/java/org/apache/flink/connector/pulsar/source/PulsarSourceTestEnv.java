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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.util.PulsarAdminUtils;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Base class for PulsarSource unit tests.
 */
public class PulsarSourceTestEnv extends PulsarTestBase {
    public static Configuration configuration = new Configuration();
    public static ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
    public static ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();
    private static PulsarAdmin pulsarAdmin;
    private static PulsarClient pulsarClient;

    public static void setup() throws Throwable {
        prepare();
        configuration.set(PulsarSourceOptions.ADMIN_URL, adminUrl);
        clientConfigurationData.setServiceUrl(serviceUrl);
        consumerConfigurationData.setSubscriptionMode(SubscriptionMode.NonDurable);
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfigurationData.setSubscriptionName("flink-" + UUID.randomUUID());
        pulsarAdmin = getPulsarAdmin();
        pulsarClient = getPulsarClient();
    }

    public static void tearDown() throws Exception {
        pulsarAdmin.close();
        pulsarClient.close();
        shutDownServices();
    }

    // --------------------- public client related helpers ------------------

    public static PulsarAdmin getPulsarAdmin() {
		if (pulsarAdmin != null) {
			return pulsarAdmin;
		}
        try {
            return PulsarAdminUtils.newAdminFromConf(adminUrl, clientConfigurationData);
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar admin", e);
        }
    }

    public static PulsarClient getPulsarClient() {
		if (pulsarClient != null) {
			return pulsarClient;
		}
        try {
            return new ClientBuilderImpl(clientConfigurationData).build();
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar client", e);
        }
    }

    // ------------------- topic information helpers -------------------

    public static void createTestTopic(String topic, int numberOfPartitions) throws Exception {
        if (numberOfPartitions == 0) {
            pulsarAdmin.topics().createNonPartitionedTopic(topic);
        } else {
            pulsarAdmin.topics().createPartitionedTopic(topic, numberOfPartitions);
        }

    }

    public static List<Partition> getPartitionsForTopic(String topic) throws Exception {
        return pulsarClient.getPartitionsForTopic(topic).get()
                .stream()
                .map(pi -> new Partition(pi, Partition.AUTO_KEY_RANGE))
                .collect(Collectors.toList());
    }

    public static Map<Integer, Map<String, PulsarPartitionSplit>> getSplitsByOwners(
            final Collection<String> topics,
            final int numSubtasks) throws Exception {
        final Map<Integer, Map<String, PulsarPartitionSplit>> splitsByOwners = new HashMap<>();
        for (String topic : topics) {
            getPartitionsForTopic(topic).forEach(partition -> {
                int ownerReader = Math.abs(partition.hashCode()) % numSubtasks;
                PulsarPartitionSplit split = new PulsarPartitionSplit(
                        partition, StartOffsetInitializer.earliest(), StopCondition.stopAfterLast());
                splitsByOwners
                        .computeIfAbsent(ownerReader, r -> new HashMap<>())
                        .put(partition.toString(), split);
            });
        }
        return splitsByOwners;
    }
}
