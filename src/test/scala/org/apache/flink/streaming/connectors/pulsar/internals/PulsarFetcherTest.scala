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
package org.apache.flink.streaming.connectors.pulsar.internals

import org.apache.flink.core.testutils.{CheckedThread, OneShotLatch}
import org.apache.flink.pulsar.PulsarFunSuite
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.pulsar.internal.{PulsarCommitCallback, PulsarFetcher}
import org.apache.flink.streaming.connectors.pulsar.testutils.TestSourceContext
import org.apache.flink.streaming.runtime.tasks.{ProcessingTimeService, TestProcessingTimeService}
import org.apache.flink.types.Row
import org.apache.flink.util.SerializedValue
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.pulsar.common.naming.TopicName

import scala.collection.JavaConverters._

class PulsarFetcherTest extends PulsarFunSuite {

  test("ignore partition states of earliest and latest") {
    val testTopic = "tp"
    val offsets = Map(topicName(testTopic, 1) -> MessageId.earliest,
      topicName(testTopic, 2) -> MessageId.latest)

    val sourceContext = new TestSourceContext[Row]()
    val fetcher = new TestFetcher(
      sourceContext, offsets, null, null, new TestProcessingTimeService, 0, null, null)

    sourceContext.getCheckpointLock.synchronized {
      val current = fetcher.snapshotCurrentState()
      fetcher.commitOffsetToPulsar(current, new PulsarCommitCallback {
        override def onSuccess(): Unit = {}

        override def onException(throwable: Throwable): Unit = {
          throw new RuntimeException(s"callback failed $throwable")
        }
      })

      assert(fetcher.lastCommittedOffsets.isDefined)
      assert(fetcher.lastCommittedOffsets.get.size == 0)
    }
  }

  // ------------------------------------------------------------------------
  //   Record emitting tests
  // ------------------------------------------------------------------------

  test("skip corrupted record") {
    val testTopic = "tp"
    val offsets = Map(topicName(testTopic, 1) -> MessageId.latest)

    val sourceContext = new TestSourceContext[Row]()

    val fetcher = new TestFetcher(
      sourceContext, offsets, null, null, new TestProcessingTimeService, 0, null, null)

    val stateHolder = fetcher.getSubscribedTopicStates().get(0)
    fetcher.emitRecord(longRow(1L), stateHolder, dummyMessageId(1))
    fetcher.emitRecord(longRow(2L), stateHolder, dummyMessageId(2))
    assert(2L == sourceContext.getLatestElement.getValue.getField(0))
    assert(dummyMessageId(2) == stateHolder.offset)

    // emit null record
    fetcher.emitRecord(null, stateHolder, dummyMessageId(3))
    assert(2L == sourceContext.getLatestElement.getValue.getField(0))
    assert(dummyMessageId(3) == stateHolder.offset)
  }

  test("skip corrupted record with periodicWatermarks") {
    val testTopic = "tp"
    val offsets = Map(topicName(testTopic, 1) -> MessageId.latest)

    val sourceContext = new TestSourceContext[Row]()

    val processingTimeProvider = new TestProcessingTimeService

    val fetcher = new TestFetcher(
      sourceContext,
      offsets,
      new SerializedValue(new PeriodicTestLongRowExtractor()),
      null,
      processingTimeProvider, 10, null, null)

    val stateHolder = fetcher.getSubscribedTopicStates().get(0)

    fetcher.emitRecord(longRow(1L), stateHolder, dummyMessageId(1))
    fetcher.emitRecord(longRow(2L), stateHolder, dummyMessageId(2))
    fetcher.emitRecord(longRow(3L), stateHolder, dummyMessageId(3))
    assert(3L == sourceContext.getLatestElement.getValue.getField(0))
    assert(3L == sourceContext.getLatestElement.getTimestamp)
    assert(dummyMessageId(3) == stateHolder.offset)

    // advance timer for watermark emitting
    processingTimeProvider.setCurrentTime(10L)
    assert(sourceContext.hasWatermark)
    assert(sourceContext.getLatestWatermark.getTimestamp == 3L)

    // emit null record
    fetcher.emitRecord(null, stateHolder, dummyMessageId(4))

    // no elements should have been collected
    assert(3L == sourceContext.getLatestElement.getValue.getField(0))
    assert(3L == sourceContext.getLatestElement.getTimestamp)
    // the offset in state still should have advanced
    assert(dummyMessageId(4) == stateHolder.offset)

    // no watermarks should be collected
    processingTimeProvider.setCurrentTime(20L)
    assert(!sourceContext.hasWatermark)
  }

  test("skip corrupted record with punctuatedWatermarks") {
    val testTopic = "tp"
    val offsets = Map(topicName(testTopic, 1) -> MessageId.latest)

    val sourceContext = new TestSourceContext[Row]()

    val fetcher = new TestFetcher(
      sourceContext,
      offsets,
      null,
      new SerializedValue(new PunctuatedTestLongRowExtractor()),
      new TestProcessingTimeService, 0, null, null)

    val stateHolder = fetcher.getSubscribedTopicStates().get(0)

    // elements generate a watermark if the timestamp is a multiple of three
    fetcher.emitRecord(longRow(1L), stateHolder, dummyMessageId(1))
    fetcher.emitRecord(longRow(2L), stateHolder, dummyMessageId(2))
    fetcher.emitRecord(longRow(3L), stateHolder, dummyMessageId(3))
    assert(3L == sourceContext.getLatestElement.getValue.getField(0))
    assert(3L == sourceContext.getLatestElement.getTimestamp)
    assert(sourceContext.hasWatermark)
    assert(3L == sourceContext.getLatestWatermark.getTimestamp)
    assert(dummyMessageId(3) == stateHolder.offset)

    // emit null record
    fetcher.emitRecord(null, stateHolder, dummyMessageId(4))

    // no elements or watermarks should have been collected
    assert(3L == sourceContext.getLatestElement.getValue.getField(0))
    assert(3L == sourceContext.getLatestElement.getTimestamp)
    assert(!sourceContext.hasWatermark)
    // the offset in state still should have advanced
    assert(dummyMessageId(4) == stateHolder.offset)
  }

  // ------------------------------------------------------------------------
  //   Timestamps & watermarks tests
  // ------------------------------------------------------------------------

  test("test periodic watermarks") {
    val testTopic = "tp"
    val offsets = Map(topicName(testTopic, 1) -> MessageId.latest,
      topicName(testTopic, 2) -> MessageId.latest,
      topicName(testTopic, 3) -> MessageId.latest)

    val sourceContext = new TestSourceContext[Row]()
    val processingTimeService = new TestProcessingTimeService()

    val fetcher = new TestFetcher(
      sourceContext,
      offsets,
      new SerializedValue(new PeriodicTestLongRowExtractor()),
      null,
      processingTimeService, 10, null, null)

    val part1 = fetcher.getSubscribedTopicStates().get(0)
    val part2 = fetcher.getSubscribedTopicStates().get(1)
    val part3 = fetcher.getSubscribedTopicStates().get(2)

    // elements for partition 1
    fetcher.emitRecord(longRow(1L), part1, dummyMessageId(1))
    fetcher.emitRecord(longRow(2L), part1, dummyMessageId(2))
    fetcher.emitRecord(longRow(3L), part1, dummyMessageId(3))
    assert(3L == sourceContext.getLatestElement.getValue.getField(0))
    assert(3L == sourceContext.getLatestElement.getTimestamp())

    fetcher.emitRecord(longRow(12L), part2, dummyMessageId(1))
    assert(12L == sourceContext.getLatestElement.getValue.getField(0))
    assert(12L == sourceContext.getLatestElement.getTimestamp())

    // element for partition 3
    fetcher.emitRecord(longRow(101L), part3, dummyMessageId(1))
    fetcher.emitRecord(longRow(102L), part3, dummyMessageId(2))
    assert(102L == sourceContext.getLatestElement.getValue.getField(0))
    assert(102L == sourceContext.getLatestElement.getTimestamp)

    processingTimeService.setCurrentTime(10)

    // now, we should have a watermark (this blocks until the periodic thread emitted the watermark)
    assert(3L == sourceContext.getLatestWatermark.getTimestamp)

    // advance partition 3
    fetcher.emitRecord(longRow(1003L), part3, dummyMessageId(3))
    fetcher.emitRecord(longRow(1004L), part3, dummyMessageId(4))
    fetcher.emitRecord(longRow(1005L), part3, dummyMessageId(5))
    assert(1005L == sourceContext.getLatestElement.getValue.getField(0))
    assert(1005L == sourceContext.getLatestElement.getTimestamp)

    // advance partition 1 beyond partition 2 - this bumps the watermark
    fetcher.emitRecord(longRow(30L), part1, dummyMessageId(4))
    assert(30L == sourceContext.getLatestElement.getValue.getField(0))
    assert(30L == sourceContext.getLatestElement.getTimestamp)

    processingTimeService.setCurrentTime(20)

    // this blocks until the periodic thread emitted the watermark
    assert(12L == sourceContext.getLatestWatermark.getTimestamp)

    // advance partition 2 again - this bumps the watermark
    fetcher.emitRecord(longRow(13L), part2, dummyMessageId(2))
    fetcher.emitRecord(longRow(14L), part2, dummyMessageId(3))
    fetcher.emitRecord(longRow(15L), part2, dummyMessageId(4))

    processingTimeService.setCurrentTime(30)
    val watermarkTs = sourceContext.getLatestWatermark.getTimestamp
    assert(watermarkTs >= 13L && watermarkTs <= 15L)
  }

  test("test punctuated watermarks") {
    val testTopic = "tp"
    val offsets = Map(topicName(testTopic, 1) -> MessageId.latest,
      topicName(testTopic, 2) -> MessageId.latest,
      topicName(testTopic, 3) -> MessageId.latest)

    val sourceContext = new TestSourceContext[Row]()
    val processingTimeService = new TestProcessingTimeService()

    val fetcher = new TestFetcher(
      sourceContext,
      offsets,
      null,
      new SerializedValue(new PunctuatedTestLongRowExtractor()),
      processingTimeService, 0, null, null)

    val part1 = fetcher.getSubscribedTopicStates().get(0)
    val part2 = fetcher.getSubscribedTopicStates().get(1)
    val part3 = fetcher.getSubscribedTopicStates().get(2)

    // elements generate a watermark if the timestamp is a multiple of three

    // elements for partition 1
    fetcher.emitRecord(longRow(1L), part1, dummyMessageId(1))
    fetcher.emitRecord(longRow(2L), part1, dummyMessageId(2))
    fetcher.emitRecord(longRow(3L), part1, dummyMessageId(3))
    assert(3L == sourceContext.getLatestElement.getValue.getField(0))
    assert(3L == sourceContext.getLatestElement.getTimestamp())
    assert(!sourceContext.hasWatermark)

    // element for partition 2
    fetcher.emitRecord(longRow(12L), part2, dummyMessageId(1))
    assert(12L == sourceContext.getLatestElement.getValue.getField(0))
    assert(12L == sourceContext.getLatestElement.getTimestamp())
    assert(!sourceContext.hasWatermark)

    // element for partition 3
    fetcher.emitRecord(longRow(101L), part3, dummyMessageId(1))
    fetcher.emitRecord(longRow(102L), part3, dummyMessageId(2))
    assert(102L == sourceContext.getLatestElement.getValue.getField(0))
    assert(102L == sourceContext.getLatestElement.getTimestamp)

    // now, we should have a watermark
    assert(sourceContext.hasWatermark)
    assert(3L == sourceContext.getLatestWatermark.getTimestamp)

    // advance partition 3
    fetcher.emitRecord(longRow(1003L), part3, dummyMessageId(3))
    fetcher.emitRecord(longRow(1004L), part3, dummyMessageId(4))
    fetcher.emitRecord(longRow(1005L), part3, dummyMessageId(5))
    assert(1005L == sourceContext.getLatestElement.getValue.getField(0))
    assert(1005L == sourceContext.getLatestElement.getTimestamp)

    // advance partition 1 beyond partition 2 - this bumps the watermark
    fetcher.emitRecord(longRow(30L), part1, dummyMessageId(4))
    assert(30L == sourceContext.getLatestElement.getValue.getField(0))
    assert(30L == sourceContext.getLatestElement.getTimestamp)
    assert(sourceContext.hasWatermark)
    assert(12L == sourceContext.getLatestWatermark.getTimestamp)

    // advance partition 2 again - this bumps the watermark
    fetcher.emitRecord(longRow(13L), part2, dummyMessageId(2))
    assert(!sourceContext.hasWatermark)
    fetcher.emitRecord(longRow(14L), part2, dummyMessageId(3))
    assert(!sourceContext.hasWatermark)
    fetcher.emitRecord(longRow(15L), part2, dummyMessageId(4))
    assert(sourceContext.hasWatermark)
    assert(15L == sourceContext.getLatestWatermark.getTimestamp)
  }

  test("periodic watermarks with no subscribedPartitions should yield no watermarks") {
    val testTopic = "tp"
    val sourceContext = new TestSourceContext[Row]()
    val processingTimeService = new TestProcessingTimeService()

    val offsets = Map.empty[String, MessageId]

    val fetcher = new TestFetcher(
      sourceContext,
      offsets,
      new SerializedValue(new PeriodicTestLongRowExtractor()),
      null,
      processingTimeService, 10, null, null)

    processingTimeService.setCurrentTime(10)
    assert(!sourceContext.hasWatermark)

    fetcher.addDiscoveredTopics(Set(topicName(testTopic, 0)))
    fetcher.emitRecord(longRow(100L), fetcher.getSubscribedTopicStates().get(0), dummyMessageId(3))
    processingTimeService.setCurrentTime(20)
    assert(100L == sourceContext.getLatestWatermark.getTimestamp)
  }

  test("concurrent topic discovery and loop fetching") {
    val tp = topicName("test", 2)
    val sourceContext = new TestSourceContext[Row]()
    val initialOffsets = Map(tp -> MessageId.earliest)

    val fetchLoopWaitLatch = new OneShotLatch
    val stateIterationBlockLatch = new OneShotLatch

    val fetcher = new TestFetcher(
      sourceContext,
      initialOffsets,
      null,
      null,
      new TestProcessingTimeService,
      10,
      fetchLoopWaitLatch,
      stateIterationBlockLatch)

    val checkedThread = new CheckedThread() {
      override def go(): Unit = {
        fetcher.runFetchLoop()
      }
    }
    checkedThread.start()

    fetchLoopWaitLatch.await()
    fetcher.addDiscoveredTopics(Set(tp))

    stateIterationBlockLatch.trigger()
    checkedThread.sync()
  }

  private def longRow(l: Long): Row = {
    val r = new Row(1)
    r.setField(0, l)
    r
  }

  private def dummyMessageId(i: Int): MessageId = {
    new MessageIdImpl(5, i , -1)
  }
}

class TestFetcher(
  sourceContext: SourceContext[Row],
  seedTopicsWithInitialOffsets: Map[String, MessageId],
  watermarksPeriodic: SerializedValue[AssignerWithPeriodicWatermarks[Row]],
  watermarksPunctuated: SerializedValue[AssignerWithPunctuatedWatermarks[Row]],
  processingTimeProvider: ProcessingTimeService,
  autoWatermarkInterval: Long,
  fetchLoopWaitLatch: OneShotLatch,
  stateIterationBlockLatch: OneShotLatch)
  extends PulsarFetcher(
  sourceContext,
  seedTopicsWithInitialOffsets,
  watermarksPeriodic,
  watermarksPunctuated,
  processingTimeProvider,
  autoWatermarkInterval,
  getClass.getClassLoader, null, "", null, null, null, 0, null) {

  var lastCommittedOffsets: Option[Map[String, MessageId]] = None

  override def runFetchLoop(): Unit = {
    if (fetchLoopWaitLatch != null) {
      subscribedTopicStates.asScala.foreach {_ =>
        fetchLoopWaitLatch.trigger()
        stateIterationBlockLatch.await()
      }
    } else {
      throw new UnsupportedOperationException
    }
  }

  override def cancel(): Unit = {
    throw new UnsupportedOperationException
  }

  override def doCommitOffsetToPulsar(
    offset: Map[String, MessageId],
    offsetCommitCallback: PulsarCommitCallback): Unit = {

    lastCommittedOffsets = Some(offset)
    offsetCommitCallback.onSuccess()
  }
}

class PeriodicTestLongRowExtractor extends AssignerWithPeriodicWatermarks[Row] {
  var maxTimestamp = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimestamp)
  }

  override def extractTimestamp(t: Row, l: Long): Long = {
    val ts = t.getField(0).asInstanceOf[Long]
    maxTimestamp = Math.max(maxTimestamp, ts)
    ts
  }
}

class PunctuatedTestLongRowExtractor extends AssignerWithPunctuatedWatermarks[Row] {
  override def checkAndGetNextWatermark(lastElement: Row, extractedTimestamp: Long): Watermark = {
    if (extractedTimestamp % 3 == 0) {
      new Watermark(extractedTimestamp)
    } else null
  }

  override def extractTimestamp(element: Row, previousElementTimestamp: Long): Long = {
    element.getField(0).asInstanceOf[Long]
  }
}
