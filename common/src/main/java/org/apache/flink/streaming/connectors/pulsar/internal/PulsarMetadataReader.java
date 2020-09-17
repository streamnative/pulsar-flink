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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.ListUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.com.google.common.collect.Iterables;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Helper class that talks to Pulsar Admin API.
 * - getEarliest / Latest / Specific MessageIds
 * - guarantee message existence using subscription by setup, move and remove
 */
@Slf4j
public class PulsarMetadataReader implements AutoCloseable {

    private final String adminUrl;

    private final String subscriptionName;

    private final Map<String, String> caseInsensitiveParams;

    private final int indexOfThisSubtask;

    private final int numParallelSubtasks;

    private final PulsarAdmin admin;

    private volatile boolean closed = false;

    private Set<String> seenTopics = new HashSet<>();

    private final boolean useExternalSubscription;

    public PulsarMetadataReader(
            String adminUrl,
            ClientConfigurationData clientConf,
            String subscriptionName,
            Map<String, String> caseInsensitiveParams,
            int indexOfThisSubtask,
            int numParallelSubtasks,
            boolean useExternalSubscription) throws PulsarClientException {

        this.adminUrl = adminUrl;
        this.subscriptionName = subscriptionName;
        this.caseInsensitiveParams = caseInsensitiveParams;
        this.indexOfThisSubtask = indexOfThisSubtask;
        this.numParallelSubtasks = numParallelSubtasks;
        this.useExternalSubscription = useExternalSubscription;
        this.admin = PulsarAdminUtils.newAdminFromConf(adminUrl, clientConf);
    }

    public PulsarMetadataReader(
            String adminUrl,
            ClientConfigurationData clientConf,
            String subscriptionName,
            Map<String, String> caseInsensitiveParams,
            int indexOfThisSubtask,
            int numParallelSubtasks) throws PulsarClientException {

        this(adminUrl, clientConf, subscriptionName, caseInsensitiveParams, indexOfThisSubtask, numParallelSubtasks, false);
    }

    @Override
    public void close() {
        closed = true;
        admin.close();
    }

    public Set<String> discoverTopicChanges() throws PulsarAdminException, ClosedException {
        if (!closed) {
            Set<String> currentTopics = getTopicPartitions();
            Set<String> addedTopics = Sets.difference(currentTopics, seenTopics);
            seenTopics = currentTopics;
            return addedTopics;
        } else {
            throw new ClosedException();
        }
    }

    public List<String> listNamespaces() throws PulsarAdminException {
        List<String> tenants = admin.tenants().getTenants();
        List<String> namespaces = new ArrayList<String>();
        for (String tenant : tenants) {
            namespaces.addAll(admin.namespaces().getNamespaces(tenant));
        }
        return namespaces;
    }

    public boolean namespaceExists(String ns) throws PulsarAdminException {
        try {
            admin.namespaces().getTopics(ns);
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        }
        return true;
    }

    public void createNamespace(String ns) throws PulsarAdminException {
        String nsName = NamespaceName.get(ns).toString();
        admin.namespaces().createNamespace(nsName);
    }

    public void deleteNamespace(String ns) throws PulsarAdminException {
        String nsName = NamespaceName.get(ns).toString();
        admin.namespaces().deleteNamespace(nsName);
    }

    public List<String> getTopics(String ns) throws PulsarAdminException {
        List<String> nonPartitionedTopics = getNonPartitionedTopics(ns);
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList(ns);
        List<String> allTopics = new ArrayList<>();
        Stream.of(partitionedTopics, nonPartitionedTopics).forEach(allTopics::addAll);
        return allTopics.stream().map(t -> TopicName.get(t).getLocalName()).collect(Collectors.toList());
    }

    public boolean topicExists(String topicName) throws PulsarAdminException {
        int partitionNum = admin.topics().getPartitionedTopicMetadata(topicName).partitions;
        if (partitionNum > 0) {
            return true;
        } else {
            admin.topics().getStats(topicName);
        }
        return true;
    }

    public void deleteTopic(String topicName) throws PulsarAdminException {
        int partitionNum = admin.topics().getPartitionedTopicMetadata(topicName).partitions;
        if (partitionNum > 0) {
            admin.topics().deletePartitionedTopic(topicName, true);
        } else {
            admin.topics().delete(topicName, true);
        }
    }

    public void createTopic(String topicName, int defaultPartitionNum) throws PulsarAdminException, IncompatibleSchemaException {
        admin.topics().createPartitionedTopic(topicName, defaultPartitionNum);
    }

    public void putSchema(String topicName, SchemaInfo schemaInfo) throws IncompatibleSchemaException {
        SchemaUtils.uploadPulsarSchema(admin, topicName, schemaInfo);
    }

    public void setupCursor(Map<String, MessageId> offset, boolean failOnDataLoss) {
        // if failOnDataLoss is false, we could continue, and re-create the sub.
        if (!useExternalSubscription || !failOnDataLoss) {
            for (Map.Entry<String, MessageId> entry : offset.entrySet()) {
                try {
                    log.info("Setting up subscription {} on topic {} at position {}", subscriptionName, entry.getKey(), entry.getValue());
                    admin.topics().createSubscription(entry.getKey(), subscriptionName, entry.getValue());
                    log.info("Subscription {} on topic {} at position {} finished", subscriptionName, entry.getKey(), entry.getValue());
                } catch (PulsarAdminException.ConflictException e) {
                    log.info("Subscription {} on topic {} already exists", subscriptionName, entry.getKey());
                } catch (PulsarAdminException e) {
                    throw new RuntimeException(
                            String.format("Failed to set up cursor for %s ", TopicName.get(entry.getKey()).toString()), e);
                }
            }
        }
    }

    public void setupCursor(Map<String, MessageId> offset) {
        setupCursor(offset, true);
    }

    public void commitCursorToOffset(Map<String, MessageId> offset) {
        for (Map.Entry<String, MessageId> entry : offset.entrySet()) {
            String tp = entry.getKey();
            try {
                log.info("Committing offset {} to topic {}", entry.getValue(), tp);
                admin.topics().resetCursor(tp, subscriptionName, entry.getValue());
                log.info("Successfully committed offset {} to topic {}", entry.getValue(), tp);
            } catch (Throwable e) {
                if (e instanceof PulsarAdminException &&
                        (((PulsarAdminException) e).getStatusCode() == 404 ||
                                ((PulsarAdminException) e).getStatusCode() == 412)) {
                    log.info("Cannot commit cursor since the topic {} has been deleted during execution", tp);
                } else {
                    throw new RuntimeException(
                            String.format("Failed to commit cursor for %s", tp), e);
                }
            }
        }
    }

    public void removeCursor(Set<String> topics) {
        if (!useExternalSubscription) {
            for (String topic : topics) {
                try {
                    log.info("Removing subscription {} from topic {}", subscriptionName, topic);
                    admin.topics().deleteSubscription(topic, subscriptionName);
                    log.info("Successfully removed subscription {} from topic {}", subscriptionName, topic);
                } catch (Throwable e) {
                    if (e instanceof PulsarAdminException && ((PulsarAdminException) e).getStatusCode() == 404) {
                        log.info("Cannot remove cursor since the topic {} has been deleted during execution", topic);
                    } else {
                        throw new RuntimeException(
                                String.format("Failed to remove cursor for %s", topic), e);
                    }
                }
            }
        }
    }

    public MessageId getPositionFromSubscription(String topic, MessageId defaultPosition) {
        try {
            TopicStats topicStats = admin.topics().getStats(topic);
            if (topicStats.subscriptions.containsKey(subscriptionName)) {
                SubscriptionStats subStats = topicStats.subscriptions.get(subscriptionName);
                if (subStats.consumers.size() != 0) {
                    throw new RuntimeException("Subscription been actively used by other consumers, " +
                            "in this situation, the exactly-once semantics cannot be guaranteed.");
                } else {
                    PersistentTopicInternalStats.CursorStats c =
                            admin.topics().getInternalStats(topic).cursors.get(subscriptionName);
                    String[] ids = c.markDeletePosition.split(":", 2);
                    long ledgerId = Long.parseLong(ids[0]);
                    long entryIdInMarkDelete = Long.parseLong(ids[1]);
                    // we are getting the next mid from sub position, if the entryId is -1,
                    // it denotes we haven't read data from the ledger before,
                    // therefore no need to skip the current entry for the next position
                    long entryId = entryIdInMarkDelete == -1 ? -1 : entryIdInMarkDelete + 1;
                    int partitionIdx = TopicName.getPartitionIndex(topic);
                    return new MessageIdImpl(ledgerId, entryId, partitionIdx);
                }
            } else {
                // create sub on topic
                admin.topics().createSubscription(topic, subscriptionName, defaultPosition);
                return defaultPosition;
            }
        } catch (PulsarAdminException e) {
            throw new RuntimeException("Failed to get stats for topic " + topic, e);
        }
    }

    public SchemaInfo getPulsarSchema(List<String> topics) throws IncompatibleSchemaException {
        Set<SchemaInfo> schemas = new HashSet<>();
        if (topics.size() > 0) {
            topics.forEach(t -> schemas.add(getPulsarSchema(t)));

            if (schemas.size() != 1) {
                throw new IncompatibleSchemaException(
                        String.format("Topics to read must share identical schema, however we got %d distinct schemas [%s]",
                                schemas.size(),
                                String.join(",", schemas.stream().map(SchemaInfo::toString).collect(Collectors.toList()))),
                        null);
            }
            return Iterables.getFirst(schemas, SchemaUtils.emptySchemaInfo());
        } else {
            return SchemaUtils.emptySchemaInfo();
        }
    }

    public SchemaInfo getPulsarSchema(String topic) {
        try {
            return admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
        } catch (Throwable e) {
            if (e instanceof PulsarAdminException && ((PulsarAdminException) e).getStatusCode() == 404) {
                return BytesSchema.of().getSchemaInfo();
            } else {
                throw new RuntimeException(
                        String.format("Failed to get schema information for %s", TopicName.get(topic).toString()), e);
            }
        }
    }

    public Set<String> getTopicPartitions() throws PulsarAdminException {
        Set<String> topics = getTopicPartitionsAll();
        return topics.stream()
                .filter(t -> SourceSinkUtils.belongsTo(t, numParallelSubtasks, indexOfThisSubtask))
                .collect(Collectors.toSet());
    }

    public Set<String> getTopicPartitionsAll() throws PulsarAdminException {
        List<String> topics = getTopics();
        HashSet<String> allTopics = new HashSet<>();
        for (String topic : topics) {
            int partNum = admin.topics().getPartitionedTopicMetadata(topic).partitions;
            if (partNum == 0) {
                allTopics.add(topic);
            } else {
                for (int i = 0; i < partNum; i++) {
                    allTopics.add(topic + PulsarOptions.PARTITION_SUFFIX + i);
                }
            }
        }
        return allTopics;
    }

    public List<String> getTopics() throws PulsarAdminException {
        for (Map.Entry<String, String> e : caseInsensitiveParams.entrySet()) {
            if (PulsarOptions.TOPIC_OPTION_KEYS.contains(e.getKey())) {
                String key = e.getKey();
                if (key.equals("topic")) {
                    return Collections.singletonList(TopicName.get(e.getValue()).toString());
                } else if (key.equals("topics")) {
                    return Arrays.asList(e.getValue().split(",")).stream()
                            .filter(s -> !s.isEmpty())
                            .map(t -> TopicName.get(t).toString())
                            .collect(Collectors.toList());
                } else { // topicspattern
                    return getTopicsWithPattern(e.getValue());
                }
            }
        }
        return null;
    }

    private List<String> getTopicsWithPattern(String topicsPattern) throws PulsarAdminException {
        TopicName dest = TopicName.get(topicsPattern);
        List<String> allNonPartitionedTopics = getNonPartitionedTopics(dest.getNamespace());
        List<String> nonPartitionedMatch = topicsPatternFilter(allNonPartitionedTopics, dest.toString());

        List<String> allPartitionedTopics = admin.topics().getPartitionedTopicList(dest.getNamespace());
        List<String> partitionedMatch = topicsPatternFilter(allPartitionedTopics, dest.toString());

        return ListUtils.union(nonPartitionedMatch, partitionedMatch);
    }

    private List<String> getNonPartitionedTopics(String namespace) throws PulsarAdminException {
        return admin.topics().getList(namespace).stream()
                .filter(t -> !TopicName.get(t).isPartitioned())
                .collect(Collectors.toList());
    }

    private List<String> topicsPatternFilter(List<String> allTopics, String topicsPattern) {
        Pattern shortenedTopicsPattern = Pattern.compile(topicsPattern.split("\\:\\/\\/")[1]);
        return allTopics.stream().map(t -> TopicName.get(t).toString())
                .filter(t -> shortenedTopicsPattern.matcher(t.split("\\:\\/\\/")[1]).matches())
                .collect(Collectors.toList());
    }

    /**
     * Designate the close of the metadata reader.
     */
    public static class ClosedException extends Exception {

    }

    public MessageId getLastMessageId(String topic) {
        try {
            return this.admin.topics().getLastMessageId(topic);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    public void resetCursor(String topic, MessageId messageId) {
        try {
            this.admin.topics().resetCursor(topic, subscriptionName, messageId);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }
}
