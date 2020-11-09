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

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.common.IntegerDeserializer;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.source.MessageDeserializer;
import org.apache.flink.connector.pulsar.source.Partition;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.StopCondition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.io.Closer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link PulsarPartitionSplitReader}.
 */
public class PulsarPartitionSplitReaderTest extends PulsarTestBase{
    private static final String TOPIC1 = TopicName.get("topic1").toString();
    private static final String TOPIC2 = TopicName.get("topic2").toString();

    @BeforeClass
    public static void setup() throws Exception {
        pulsarAdmin = getPulsarAdmin();
        pulsarClient = getPulsarClient();
    }

    @Test
    public void testWakeUp() throws InterruptedException {
        PulsarPartitionSplitReader<Integer> reader = createReader();
        Partition nonExistingPartition = new Partition("NotExist", Partition.AUTO_KEY_RANGE);
        PulsarPartitionSplit pulsarPartitionSplit = new PulsarPartitionSplit(nonExistingPartition, StartOffsetInitializer.earliest(), StopCondition.stopAfterLast());
        assignSplits(
                reader,
                Collections.singletonMap(
                        pulsarPartitionSplit.splitId(),
                        pulsarPartitionSplit
                )
        );
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                reader.fetch();
            } catch (Throwable e) {
                error.set(e);
            }
        }, "testWakeUp-thread");
        t.start();
        long deadline = System.currentTimeMillis() + 5000L;
        while (t.isAlive() && System.currentTimeMillis() < deadline) {
            reader.wakeUp();
            Thread.sleep(10);
        }
        assertNull(error.get());
    }

    // ------------------

    private PulsarPartitionSplitReader<Integer> createReader() {

        PulsarClient pulsarClient = getPulsarClient();
        PulsarAdmin pulsarAdmin = getPulsarAdmin();

        ExecutorService listenerExecutor = Executors.newScheduledThreadPool(
                1,
                r -> new Thread(r, "Pulsar listener executor"));
        Closer splitCloser = Closer.create();
        splitCloser.register(listenerExecutor::shutdownNow);
        PulsarPartitionSplitReader<Integer> reader = new PulsarPartitionSplitReader<>(
                configuration,
                consumerConfigurationData,
                pulsarClient,
                pulsarAdmin,
                MessageDeserializer.valueOnly(new IntegerDeserializer()),
                listenerExecutor);
        splitCloser.register(reader);
        return reader;
    }

    private Map<String, PulsarPartitionSplit> assignSplits(
            PulsarPartitionSplitReader<Integer> reader,
            Map<String, PulsarPartitionSplit> splits) {
        SplitsChange<PulsarPartitionSplit> splitsChange = new SplitsAddition<>(new ArrayList<>(splits.values()));
        Queue<SplitsChange<PulsarPartitionSplit>> queue = new LinkedList<>();
        queue.add(splitsChange);
        reader.handleSplitsChanges(queue);
        return splits;
    }

}
