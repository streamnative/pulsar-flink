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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.TestLogger;

import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.service.PulsarServiceFactory;
import io.streamnative.tests.pulsar.service.PulsarServiceSpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.shaded.com.google.common.collect.Sets;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Start / stop a Pulsar cluster.
 */
@Slf4j
public abstract class PulsarTestBase extends TestLogger {

    protected static PulsarService pulsarService;

    protected static String serviceUrl;

    protected static String adminUrl;

    public static String getServiceUrl() {
        return serviceUrl;
    }

    public static String getAdminUrl() {
        return adminUrl;
    }

    @BeforeClass
    public static void prepare() throws Exception {

        log.info("-------------------------------------------------------------------------");
        log.info("    Starting PulsarTestBase ");
        log.info("-------------------------------------------------------------------------");

        PulsarServiceSpec spec = PulsarServiceSpec.builder()
                .clusterName("standalone-" + UUID.randomUUID())
                .enableContainerLogging(false)
                .build();

        pulsarService = PulsarServiceFactory.createPulsarService(spec);
        pulsarService.start();

        log.info("Pulsar Service Uris: {}", pulsarService.getServiceUris());

        for (URI uri : pulsarService.getServiceUris()) {
            if (uri != null && uri.getScheme().equals("pulsar")) {
                serviceUrl = uri.toString();
            } else if (uri != null && uri.getScheme().equals("http")) {
                adminUrl = uri.toString();
            }
        }

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            admin.namespaces().createNamespace("public/default", Sets.newHashSet("standalone"));
        }

        log.info("-------------------------------------------------------------------------");
        log.info("Successfully started pulsar service at cluster " + spec.clusterName());
        log.info("-------------------------------------------------------------------------");

    }

    @AfterClass
    public static void shutDownServices() throws Exception {
        log.info("-------------------------------------------------------------------------");
        log.info("    Shut down PulsarTestBase ");
        log.info("-------------------------------------------------------------------------");

        TestStreamEnvironment.unsetAsContext();

        if (pulsarService != null) {
            pulsarService.stop();
        }

        log.info("-------------------------------------------------------------------------");
        log.info("    PulsarTestBase finished");
        log.info("-------------------------------------------------------------------------");
    }

    protected static Configuration getFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "16m");
        flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "my_reporter." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
        return flinkConfig;
    }

    public static <T> List<MessageId> sendTypedMessages(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition) throws PulsarClientException {

        return sendTypedMessages(topic, type, messages, partition, null);
    }

    public static <T> List<MessageId> sendTypedMessages(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition,
            Class<T> tClass) throws PulsarClientException {

        String topicName;
        if (partition.isPresent()) {
            topicName = topic + PulsarOptions.PARTITION_SUFFIX + partition.get();
        } else {
            topicName = topic;
        }

        Producer<T> producer = null;
        PulsarClient client = null;
        List<MessageId> mids = new ArrayList<>();

        try {
            client = PulsarClient.builder().serviceUrl(getServiceUrl()).build();

            switch (type) {
                case BOOLEAN:
                    producer = (Producer<T>) client.newProducer(Schema.BOOL).topic(topicName).create();
                    break;
                case BYTES:
                    producer = (Producer<T>) client.newProducer(Schema.BYTES).topic(topicName).create();
                    break;
                case DATE:
                    producer = (Producer<T>) client.newProducer(Schema.DATE).topic(topicName).create();
                    break;
                case STRING:
                    producer = (Producer<T>) client.newProducer(Schema.STRING).topic(topicName).create();
                    break;
                case TIMESTAMP:
                    producer = (Producer<T>) client.newProducer(Schema.TIMESTAMP).topic(topicName).create();
                    break;
                case INT8:
                    producer = (Producer<T>) client.newProducer(Schema.INT8).topic(topicName).create();
                    break;
                case DOUBLE:
                    producer = (Producer<T>) client.newProducer(Schema.DOUBLE).topic(topicName).create();
                    break;
                case FLOAT:
                    producer = (Producer<T>) client.newProducer(Schema.FLOAT).topic(topicName).create();
                    break;
                case INT32:
                    producer = (Producer<T>) client.newProducer(Schema.INT32).topic(topicName).create();
                    break;
                case INT16:
                    producer = (Producer<T>) client.newProducer(Schema.INT16).topic(topicName).create();
                    break;
                case INT64:
                    producer = (Producer<T>) client.newProducer(Schema.INT64).topic(topicName).create();
                    break;
                case AVRO:
                    producer = (Producer<T>) client.newProducer(Schema.AVRO(tClass)).topic(topicName).create();
                    break;
                case JSON:
                    producer = (Producer<T>) client.newProducer(Schema.JSON(tClass)).topic(topicName).create();
                    break;

                default:
                    throw new NotImplementedException("Unsupported type " + type);
            }

            for (T message : messages) {
                MessageId mid = producer.send(message);
                log.info("Sent {} of mid: {}", message.toString(), mid.toString());
                mids.add(mid);
            }

        } finally {
            producer.flush();
            producer.close();
            client.close();
        }
        return mids;
    }
}
