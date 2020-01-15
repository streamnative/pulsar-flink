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

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.pulsar.testutils.TestSourceContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PulsarFetcherTest extends TestLogger {

    String topicName(String topic, int partition) {
        return TopicName.get(topic).getPartition(partition).toString();
    }

    private MessageId dummyMessageId(int i) {
        return new MessageIdImpl(5, i, -1);
    }

    @Test
    public void testIgnorePartitionStates() throws Exception {
        String testTopic = "tp";
        Map<String, MessageId> offset = new HashMap<>();
        offset.put(topicName(testTopic, 1), MessageId.earliest);
        offset.put(topicName(testTopic, 2), MessageId.latest);

        TestSourceContext<Long> sourceContext = new TestSourceContext<>();
        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                offset,
                null,
                null,
                new TestProcessingTimeService(),
                0,
                null,
                null);

        synchronized (sourceContext.getCheckpointLock()) {
            Map<String, MessageId> current = fetcher.snapshotCurrentState();
            fetcher.commitOffsetToPulsar(current, new PulsarCommitCallback() {
                @Override
                public void onSuccess() {
                }

                @Override
                public void onException(Throwable cause) {
                    throw new RuntimeException("Callback failed", cause);
                }
            });

            assertTrue(fetcher.lastCommittedOffsets.isPresent());
            assertEquals(fetcher.lastCommittedOffsets.get().size(), 0);
        }
    }


    @Test
    public void testSkipCorruptedRecord() throws Exception {
        String testTopic = "tp";
        Map<String, MessageId> offset = Collections.singletonMap(topicName(testTopic, 1), MessageId.latest);

        TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                offset,
                null,
                null,
                new TestProcessingTimeService(),
                0,
                null,
                null);

        PulsarTopicState stateHolder = fetcher.getSubscribedTopicStates().get(0);
        fetcher.emitRecord(1L, stateHolder, dummyMessageId(1));
        fetcher.emitRecord(2L, stateHolder, dummyMessageId(2));
        assertEquals(2L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(dummyMessageId(2), stateHolder.getOffset());

        // emit null record
        fetcher.emitRecord(null, stateHolder, dummyMessageId(3));
        assertEquals(2L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(dummyMessageId(3), stateHolder.getOffset());
    }

    @Test
    public void testSkipCorruptedRecordWithPeriodicWatermarks() throws Exception {
        String testTopic = "tp";
        Map<String, MessageId> offset = Collections.singletonMap(topicName(testTopic, 1), MessageId.latest);

        TestProcessingTimeService processingTimeProvider = new TestProcessingTimeService();

        TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                offset,
                new SerializedValue<>(new PeriodicTestExtractor()), /* periodic watermark assigner */
                null,
                processingTimeProvider,
                10,
                null,
                null);

        PulsarTopicState stateHolder = fetcher.getSubscribedTopicStates().get(0);

        fetcher.emitRecord(1L, stateHolder, dummyMessageId(1));
        fetcher.emitRecord(2L, stateHolder, dummyMessageId(2));
        fetcher.emitRecord(3L, stateHolder, dummyMessageId(3));
        assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
        assertEquals(dummyMessageId(3), stateHolder.getOffset());

        // advance timer for watermark emitting
        processingTimeProvider.setCurrentTime(10L);
        assertTrue(sourceContext.hasWatermark());
        assertEquals(sourceContext.getLatestElement().getTimestamp(), 3L);

        // emit null record
        fetcher.emitRecord(null, stateHolder, dummyMessageId(4));

        // no elements should have been collected
        assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
        // the offset in state still should be advanced
        assertEquals(dummyMessageId(4), stateHolder.getOffset());

        processingTimeProvider.setCurrentTime(20L);
        assertTrue(!sourceContext.hasWatermark());
    }

    @Test
    public void testSkipCorruptedRecordWithPunctuatedWatermarks() throws Exception {
        String testTopic = "tp";
        Map<String, MessageId> offset = Collections.singletonMap(topicName(testTopic, 1), MessageId.latest);

        TestProcessingTimeService processingTimeProvider = new TestProcessingTimeService();

        TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                offset,
                null,
                new SerializedValue<>(new PunctuatedTestExtractor()),
                new TestProcessingTimeService(),
                0,
                null,
                null);

        PulsarTopicState stateHolder = fetcher.getSubscribedTopicStates().get(0);

        // elements generate a watermark if the timestamp is a multiple of three
        fetcher.emitRecord(1L, stateHolder, dummyMessageId(1));
        fetcher.emitRecord(2L, stateHolder, dummyMessageId(2));
        fetcher.emitRecord(3L, stateHolder, dummyMessageId(3));
        assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
        assertTrue(sourceContext.hasWatermark());
        assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());
        assertEquals(dummyMessageId(3), stateHolder.getOffset());

        // emit null record
        fetcher.emitRecord(null, stateHolder, dummyMessageId(4));

        // no elements should have been collected
        assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
        assertTrue(!sourceContext.hasWatermark());
        // the offset in state still should be advanced
        assertEquals(dummyMessageId(4), stateHolder.getOffset());
    }

    @Test
    public void testPeriodicWatermarks() throws Exception {
        String testTopic = "tp";
        Map<String, MessageId> offset = new HashMap<>();
        offset.put(topicName(testTopic, 1), MessageId.latest);
        offset.put(topicName(testTopic, 2), MessageId.latest);
        offset.put(topicName(testTopic, 3), MessageId.latest);

        TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
        TestProcessingTimeService processingTimeProvider = new TestProcessingTimeService();
        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                offset,
                new SerializedValue<>(new PeriodicTestExtractor()), /* periodic watermark assigner */
                null,
                processingTimeProvider,
                10,
                null,
                null);

        PulsarTopicState part1 = fetcher.getSubscribedTopicStates().get(0);
        PulsarTopicState part2 = fetcher.getSubscribedTopicStates().get(1);
        PulsarTopicState part3 = fetcher.getSubscribedTopicStates().get(2);

        // elements for partition 1
        fetcher.emitRecord(1L, part1, dummyMessageId(1));
        fetcher.emitRecord(2L, part1, dummyMessageId(2));
        fetcher.emitRecord(3L, part1, dummyMessageId(3));
        assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(3L, sourceContext.getLatestElement().getTimestamp());

        fetcher.emitRecord(12L, part2, dummyMessageId(1));
        assertEquals(12L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(12L, sourceContext.getLatestElement().getTimestamp());

        // element for partition 3
        fetcher.emitRecord(101L, part3, dummyMessageId(1));
        fetcher.emitRecord(102L, part3, dummyMessageId(2));
        assertEquals(102L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(102L, sourceContext.getLatestElement().getTimestamp());

        processingTimeProvider.setCurrentTime(10);

        // now, we should have a watermark (this blocks until the periodic thread emitted the watermark)
        assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());

        // advance partition 3
        fetcher.emitRecord(1003L, part3, dummyMessageId(3));
        fetcher.emitRecord(1004L, part3, dummyMessageId(4));
        fetcher.emitRecord(1005L, part3, dummyMessageId(5));
        assertEquals(1005L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(1005L, sourceContext.getLatestElement().getTimestamp());

        // advance partition 1 beyond partition 2 - this bumps the watermark
        fetcher.emitRecord(30L, part1, dummyMessageId(4));
        assertEquals(30L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(30L, sourceContext.getLatestElement().getTimestamp());

        processingTimeProvider.setCurrentTime(20);

        // this blocks until the periodic thread emitted the watermark
        assertEquals(12L, sourceContext.getLatestWatermark().getTimestamp());

        // advance partition 2 again - this bumps the watermark
        fetcher.emitRecord(13L, part2, dummyMessageId(2));
        fetcher.emitRecord(14L, part2, dummyMessageId(3));
        fetcher.emitRecord(15L, part2, dummyMessageId(4));

        processingTimeProvider.setCurrentTime(30);
        long watermarkTs = sourceContext.getLatestWatermark().getTimestamp();
        assertTrue(watermarkTs >= 13L && watermarkTs <= 15L);

    }

    @Test
    public void testPunctuatedWatermarks() throws Exception {
        String testTopic = "tp";
        Map<String, MessageId> offset = new HashMap<>();
        offset.put(topicName(testTopic, 1), MessageId.latest);
        offset.put(topicName(testTopic, 2), MessageId.latest);
        offset.put(topicName(testTopic, 3), MessageId.latest);

        TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                offset,
                null,
                new SerializedValue<>(new PunctuatedTestExtractor()),
                new TestProcessingTimeService(),
                0,
                null,
                null);

        PulsarTopicState part1 = fetcher.getSubscribedTopicStates().get(0);
        PulsarTopicState part2 = fetcher.getSubscribedTopicStates().get(1);
        PulsarTopicState part3 = fetcher.getSubscribedTopicStates().get(2);

        // elements generate a watermark if the timestamp is a multiple of three

        // elements for partition 1
        fetcher.emitRecord(1L, part1, dummyMessageId(1));
        fetcher.emitRecord(2L, part1, dummyMessageId(2));
        fetcher.emitRecord(3L, part1, dummyMessageId(3));
        assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
        assertTrue(!sourceContext.hasWatermark());

        // element for partition 2
        fetcher.emitRecord(12L, part2, dummyMessageId(1));
        assertEquals(12L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(12L, sourceContext.getLatestElement().getTimestamp());
        assertTrue(!sourceContext.hasWatermark());

        // element for partition 3
        fetcher.emitRecord(101L, part3, dummyMessageId(1));
        fetcher.emitRecord(102L, part3, dummyMessageId(2));
        assertEquals(102L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(102L, sourceContext.getLatestElement().getTimestamp());

        // now, we should have a watermark
        assertTrue(sourceContext.hasWatermark());
        assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());

        // advance partition 3
        fetcher.emitRecord(1003L, part3, dummyMessageId(3));
        fetcher.emitRecord(1004L, part3, dummyMessageId(4));
        fetcher.emitRecord(1005L, part3, dummyMessageId(5));
        assertEquals(1005L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(1005L, sourceContext.getLatestElement().getTimestamp());

        // advance partition 1 beyond partition 2 - this bumps the watermark
        fetcher.emitRecord(30L, part1, dummyMessageId(4));
        assertEquals(30L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(30L, sourceContext.getLatestElement().getTimestamp());
        assertTrue(sourceContext.hasWatermark());
        assertEquals(12L, sourceContext.getLatestWatermark().getTimestamp());

        // advance partition 2 again - this bumps the watermark
        fetcher.emitRecord(13L, part2, dummyMessageId(2));
        assertTrue(!sourceContext.hasWatermark());
        fetcher.emitRecord(14L, part2, dummyMessageId(3));
        assertTrue(!sourceContext.hasWatermark());
        fetcher.emitRecord(15L, part2, dummyMessageId(4));
        assertTrue(sourceContext.hasWatermark());
        assertEquals(15L, sourceContext.getLatestWatermark().getTimestamp());
    }

    @Test
    public void testPeriodicWatermarksWithNoSubscribedPartitionsShouldYieldNoWatermarks() throws Exception {
        String testTopic = "tp";
        TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

        Map<String, MessageId> offset = new HashMap<>();

        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                offset,
                new SerializedValue<>(new PeriodicTestExtractor()), /* periodic watermark assigner */
                null,
                processingTimeService,
                10,
                null,
                null);

        processingTimeService.setCurrentTime(10);
        assertTrue(!sourceContext.hasWatermark());

        fetcher.addDiscoveredTopics(Sets.newSet(topicName(testTopic, 0)));
        fetcher.emitRecord(100L, fetcher.getSubscribedTopicStates().get(0), dummyMessageId(3));
        processingTimeService.setCurrentTime(20);
        assertEquals(100L, sourceContext.getLatestWatermark().getTimestamp());
    }

    @Test
    public void testConcurrentPartitionsDiscoveryAndLoopFetching() throws Exception {
        String tp = topicName("test", 2);
        TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
        Map<String, MessageId> offset = Collections.singletonMap(topicName(tp, 1), MessageId.latest);

        OneShotLatch fetchLoopWaitLatch = new OneShotLatch();
        OneShotLatch stateIterationBlockLatch = new OneShotLatch();

        TestFetcher fetcher = new TestFetcher(
                sourceContext,
                offset,
                null,
                null,
                new TestProcessingTimeService(),
                10,
                fetchLoopWaitLatch,
                stateIterationBlockLatch);


        // ----- run the fetcher -----

        final CheckedThread checkedThread = new CheckedThread() {
            @Override
            public void go() throws Exception {
                fetcher.runFetchLoop();
            }
        };
        checkedThread.start();

        fetchLoopWaitLatch.await();
        fetcher.addDiscoveredTopics(Sets.newSet(tp));

        stateIterationBlockLatch.trigger();
        checkedThread.sync();
    }

    private static final class TestFetcher<T> extends PulsarFetcher<T> {

        private final OneShotLatch fetchLoopWaitLatch;
        private final OneShotLatch stateIterationBlockLatch;
        Optional<Map<String, MessageId>> lastCommittedOffsets = Optional.empty();

        public TestFetcher(
                SourceFunction.SourceContext<T> sourceContext,
                Map<String, MessageId> seedTopicsWithInitialOffsets,
                SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
                SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
                ProcessingTimeService processingTimeProvider,
                long autoWatermarkInterval,
                OneShotLatch fetchLoopWaitLatch,
                OneShotLatch stateIterationBlockLatch) throws Exception {
            super(
                    sourceContext,
                    seedTopicsWithInitialOffsets,
                    watermarksPeriodic,
                    watermarksPunctuated,
                    processingTimeProvider,
                    autoWatermarkInterval,
                    TestFetcher.class.getClassLoader(),
                    null,
                    null,
                    null,
                    0,
                    null,
                    null);

            this.fetchLoopWaitLatch = fetchLoopWaitLatch;
            this.stateIterationBlockLatch = stateIterationBlockLatch;
        }

        @Override
        public void runFetchLoop() throws Exception {
            if (fetchLoopWaitLatch != null) {
                for (PulsarTopicState state : subscribedPartitionStates) {
                    fetchLoopWaitLatch.trigger();
                    stateIterationBlockLatch.await();
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void cancel() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doCommitOffsetToPulsar(
                Map<String, MessageId> offset,
                PulsarCommitCallback offsetCommitCallback) {

            lastCommittedOffsets = Optional.of(offset);
            offsetCommitCallback.onSuccess();
        }
    }

    private static class PeriodicTestExtractor implements AssignerWithPeriodicWatermarks<Long> {

        private volatile long maxTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(Long element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element);
            return element;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }
    }

    private static class PunctuatedTestExtractor implements AssignerWithPunctuatedWatermarks<Long> {

        @Override
        public long extractTimestamp(Long element, long previousElementTimestamp) {
            return element;
        }

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
            return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
        }
    }
}