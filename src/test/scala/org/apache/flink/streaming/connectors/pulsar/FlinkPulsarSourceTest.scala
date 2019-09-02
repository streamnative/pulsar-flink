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
package org.apache.flink.streaming.connectors.pulsar

import java.{io, util}
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.function.Predicate

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapStateDescriptor, OperatorStateStore}
import org.apache.flink.api.java.tuple.{Tuple, Tuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.testutils.{CheckedThread, OneShotLatch}
import org.apache.flink.pulsar.ClosedException
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState
import org.apache.flink.runtime.state.{FunctionInitializationContext, StateSnapshotContextSynchronousImpl}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.operators.{StreamSource, StreamingRuntimeContext}
import org.apache.flink.streaming.connectors.pulsar.internal.{ClosedException, PulsarCommitCallback, PulsarFetcher, PulsarMetadataReader}
import org.apache.flink.streaming.connectors.pulsar.internals.{PulsarFunSuite, TestMetadataReader}
import org.apache.flink.streaming.connectors.pulsar.testutils.TestSourceContext
import org.apache.flink.streaming.runtime.tasks.{ProcessingTimeService, TestProcessingTimeService}
import org.apache.flink.streaming.util.{AbstractStreamOperatorTestHarness, MockStreamingRuntimeContext}
import org.apache.flink.types.Row
import org.apache.flink.util.{ExceptionUtils, FlinkException, Preconditions, SerializedValue}
import org.apache.flink.util.function.{SupplierWithException, ThrowingRunnable}
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.MessageIdImpl
import org.mockito.Mockito.{doThrow, mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._
import scala.collection.mutable

class FlinkPulsarSourceTest extends PulsarFunSuite with MockitoSugar {

  val maxParallelism = Short.MaxValue / 2

  test("either watermark extractor") {
    intercept[NullPointerException] {
      new DummyFlinkPulsarSource().assignTimestampsAndWatermarks(
        null.asInstanceOf[AssignerWithPeriodicWatermarks[Row]])
    }

    intercept[NullPointerException] {
      new DummyFlinkPulsarSource().assignTimestampsAndWatermarks(
        null.asInstanceOf[AssignerWithPunctuatedWatermarks[Row]])
    }

    val periodic = mock[AssignerWithPeriodicWatermarks[Row]]
    val punctuated = mock[AssignerWithPunctuatedWatermarks[Row]]

    val c1 = new DummyFlinkPulsarSource()
    c1.assignTimestampsAndWatermarks(periodic)

    intercept[IllegalStateException] {
      c1.assignTimestampsAndWatermarks(punctuated)
    }

    val c2 = new DummyFlinkPulsarSource()
    c2.assignTimestampsAndWatermarks(punctuated)

    intercept[IllegalStateException] {
      c2.assignTimestampsAndWatermarks(periodic)
    }
  }

  test("ignore checkpoint when not running") {
    val fetcher = new MockFetcher(Seq())
    val source = new DummyFlinkPulsarSource(fetcher, mockDiscoverer())

    val listState = new TestingListState[Tuple]
    setupSource(source, false, listState, true, 0, 1)

    source.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1))
    assert(!listState.get.iterator().hasNext)

    source.notifyCheckpointComplete(1L)
    assert(fetcher.getAndClearLastCommittedOffsets() == null)
    assert(fetcher.getCommitCount() == 0)
  }

  /**
   * Tests that when taking a checkpoint when the fetcher is not running yet,
   * the checkpoint correctly contains the restored state instead.
   */
  test("check restored checkpoint when fetcher not ready") {
    val source = new DummyFlinkPulsarSource(mockDiscoverer())
    val listState = new TestingListState[Tuple]
    setupSource(source, true, listState, true, 0, 1)

    // snapshot before the fetcher starts running
    source.snapshotState(new StateSnapshotContextSynchronousImpl(17, 17))

    assert(listState.isClearCalled)
    val expected = mutable.Set.empty[java.io.Serializable]
    listState.get.asScala.foreach(expected.add)

    var counter = 0
    for (elem <- listState.get.asScala) {
      assert(expected.contains(elem))
      counter += 1
    }
    assert(expected.size == counter)
  }

  test("snapshot state with commit on checkpoints") {
    // fake states
    val state1 = Map("abc" -> dummyMessageId(5), "def" -> dummyMessageId(90))
    val state2 = Map("abc" -> dummyMessageId(10), "def" -> dummyMessageId(95))
    val state3 = Map("abc" -> dummyMessageId(15), "def" -> dummyMessageId(100))

    val fetcher = new MockFetcher(state1.asJava :: state2.asJava :: state3.asJava :: Nil)

    val source = new DummyFlinkPulsarSource(fetcher, mockDiscoverer())
    val listState = new TestingListState[Tuple2[String, MessageId]]
    setupSource(source, false, listState, true, 0, 1)

    val runThread = new CheckedThread() {
      override def go(): Unit = {
        source.run(new TestSourceContext[Row])
      }
    }
    runThread.start()
    fetcher.waitUntilRun()
    assert(0 == source.getPendingOffsetsToCommit().size())

    // checkpoint 1
    source.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138))
    val snap1 = listState.get.asScala.map(t => t.f0 -> t.f1).toMap
    assert(snap1 == state1)
    assert(1 == source.getPendingOffsetsToCommit().size())

    // checkpoint 2
    source.snapshotState(new StateSnapshotContextSynchronousImpl(140, 140))
    val snap2 = listState.get.asScala.map(t => t.f0 -> t.f1).toMap
    assert(snap2 == state2)
    assert(2 == source.getPendingOffsetsToCommit().size())

    // ack checkpoint 1
    source.notifyCheckpointComplete(138L)
    assert(1 == fetcher.getCommitCount())
    assert(fetcher.getAndClearLastCommittedOffsets() != null)

    // checkpoint 3
    source.snapshotState(new StateSnapshotContextSynchronousImpl(141, 141))
    val snap3 = listState.get.asScala.map(t => t.f0 -> t.f1).toMap
    assert(snap3 == state3)
    assert(2 == source.getPendingOffsetsToCommit().size())

    source.notifyCheckpointComplete(141L)
    assert(2 == fetcher.getCommitCount())
    assert(fetcher.getAndClearLastCommittedOffsets() != null)

    source.notifyCheckpointComplete(666)
    assert(2 == fetcher.getCommitCount())
    assert(fetcher.getAndClearLastCommittedOffsets() == null)

    source.cancel()
    runThread.sync()
  }

  test("close discoverer when open throw exception") {
    val failureCause = new RuntimeException(new FlinkException("test exception"))
    val failingPartitionDiscoverer = new FailingPartitionDiscoverer(failureCause)
    val source = new DummyFlinkPulsarSource(failingPartitionDiscoverer)
    testFailingSourceLifecycle(source, failureCause)
    assert(failingPartitionDiscoverer.isClosed())
  }

  test("close discoverer when create fetcher fails") {
    val failureCause = new FlinkException("create fetcher failure")
    val discoverer = new DummyPartitionDiscoverer
    val source = new DummyFlinkPulsarSource(new SupplierWithException[PulsarFetcher, Exception] {
      override def get(): PulsarFetcher = throw failureCause
    },
      discoverer, 100)
    testFailingSourceLifecycle(source, failureCause)
    assert(discoverer.isClosed())
  }

  test("close discoverer when fetcher fails") {
    val failure = new FlinkException("Run fetcher failure")
    val discoverer = new DummyPartitionDiscoverer
    val mockFetcher = mock[PulsarFetcher]
    when(mockFetcher.runFetchLoop()).thenThrow(failure)
    val source = new DummyFlinkPulsarSource(new SupplierWithException[PulsarFetcher, Exception] {
      override def get(): PulsarFetcher = mockFetcher
    }, discoverer, 100)
    testFailingSourceLifecycle(source, failure)
    assert(discoverer.isClosed())
  }

  test("close discoverer with cancellation") {
    val discoverer = new DummyPartitionDiscoverer
    val source = new TestingFlinkPulsarSource(discoverer, 100L)
    testNormalSourceLifecycle(source)
    assert(discoverer.isClosed())
  }

  test("test scale up") {
    testRescaling(5, 2, 8, 30)
  }

  test("test scale down") {
    testRescaling(5, 10, 2, 100)
  }

  private def testRescaling(
    initialParallelism: Int,
    numPartitions: Int,
    restoredParallelism: Int,
    restoredNumPartitions: Int): Unit = {

    val startupTopics = (0 until numPartitions).map(topicName("test-topic", _))
    val sourceAndHarness = (0 until initialParallelism).map { i =>
      val discoverer = new TestMetadataReader(
        Map("topic" -> "test-topic"),
        i,
        initialParallelism,
        TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(startupTopics.toSet)
      )
      val source = new DummyFlinkPulsarSource(discoverer)
      val harness = createTestHarness(source, initialParallelism, i)
      harness.initializeEmptyState()
      harness.open()
      (source, harness)
    }

    val (sources, harnesses) = sourceAndHarness.unzip

    val globalsSeq = (0 until initialParallelism).flatMap { i =>
      sources(i).ownedTopicStarts.toMap
    }

    assert(globalsSeq.size == numPartitions)
    assert(startupTopics.forall(globalsSeq.toMap.contains(_)))

    val states = (0 until initialParallelism).map {i =>
      harnesses(i).snapshot(0, 0)
    }

    val mergedState = AbstractStreamOperatorTestHarness.repackageState(states: _*)

    // restore

    val restoredTopics = (0 until restoredNumPartitions).map(topicName("test-topic", _))
    val rSourceAndHarness = (0 until restoredParallelism).map { i =>
      val initState = AbstractStreamOperatorTestHarness.repartitionOperatorState(
        mergedState, maxParallelism, initialParallelism, restoredParallelism, i)
      val discoverer = new TestMetadataReader(
        Map("topic" -> "test-topic"),
        i,
        restoredParallelism,
        TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(restoredTopics.toSet)
      )
      val source = new DummyFlinkPulsarSource(discoverer)
      val harness = createTestHarness(source, restoredParallelism, i)
      harness.initializeState(initState)
      harness.open()
      (source, harness)
    }

    val (rsources, rharnesses) = rSourceAndHarness.unzip

    val rGlobalsSeq = (0 until restoredParallelism).flatMap { i =>
      rsources(i).ownedTopicStarts.toMap
    }

    assert(rGlobalsSeq.size == restoredNumPartitions)
    assert(restoredTopics.forall(rGlobalsSeq.toMap.contains(_)))
  }

  private def createTestHarness[T](
    source: SourceFunction[T],
    numSubtasks: Int,
    subtaskIndex: Int) = {
    val testHarness = new AbstractStreamOperatorTestHarness[T](
      new StreamSource[T, SourceFunction[T]](source),
      maxParallelism,
      numSubtasks,
      subtaskIndex)
    testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime)
    testHarness
  }


  private def testNormalSourceLifecycle(
    source: TestingFlinkPulsarSource) = {
    setupSource(source)
    val runFuture = CompletableFuture.runAsync(ThrowingRunnable.unchecked(new ThrowingRunnable[Throwable] {
      override def run(): Unit = source.run(new TestSourceContext[Row])
    }))
    source.close()
    runFuture.get()
  }

  private def testFailingSourceLifecycle(source: FlinkPulsarSource, excp: Exception): Unit = {
    try {
      setupSource(source)
      source.run(new TestSourceContext[Row])
      fail("should have failed from open or run")
    } catch {
      case e: Exception =>
        assert(ExceptionUtils.findThrowable(e, new Predicate[Throwable] {
          override def test(t: Throwable): Boolean = t.equals(excp)
        }).isPresent)
    }
    source.close()
  }


  private def dummyMessageId(i: Int): MessageId = {
    new MessageIdImpl(5, i , -1)
  }

  def mockDiscoverer(): PulsarMetadataReader = {
    val discoverer = mock[PulsarMetadataReader]
    when(discoverer.discoverTopicsChange()).thenAnswer(new Answer[Set[String]] {
      override def answer(
        invocation: InvocationOnMock): Set[String] = Set.empty
    })
    discoverer
  }

  def setupSource(source: FlinkPulsarSource): Unit = {
    setupSource(source, false, null, false, 0, 1)
  }

  def setupSource[S](
    source: FlinkPulsarSource,
    isRestored: Boolean,
    restoredListState: ListState[S],
    isCheckpointingEnabled: Boolean,
    subtaskIndex: Int,
    totalNumSubtasks: Int): Unit = {

    source.setRuntimeContext(
      new MockStreamingRuntimeContext(
        isCheckpointingEnabled, totalNumSubtasks, subtaskIndex))
    source.initializeState(
      new MockFunctionInitializationContext(
        isRestored, new MockOperatorStateStore(restoredListState)))
    source.open(new Configuration())
  }
}

class FailingPartitionDiscoverer(failureCause: RuntimeException)
  extends PulsarMetadataReader("", null, "", null, 0, 1) {

  @volatile private var closed = false

  override def getTopicPartitionsAll(): Set[String] = null

  override def discoverTopicsChange(): Set[String] = {
    throw failureCause
  }

  def isClosed(): Boolean = closed

  override def close(): Unit = {
    closed = true
  }
}

class DummyPartitionDiscoverer
  extends PulsarMetadataReader("", null, "", null, 0, 1) {

  val allPartitions: Set[String] = Set("foo")

  @volatile private var closed = false

  override def getTopicPartitionsAll(): Set[String] = {
    checkState()
    allPartitions
  }

  private def checkState(): Unit = {
    if (closed) throw new ClosedException
  }

  def isClosed(): Boolean = closed

  override def close(): Unit = {
    closed = true
  }
}

class TestingFetcher(
    sourceContext: SourceContext[Row],
    seedTopicsWithInitialOffsets: Map[String, MessageId],
    watermarksPeriodic: SerializedValue[AssignerWithPeriodicWatermarks[Row]],
    watermarksPunctuated: SerializedValue[AssignerWithPunctuatedWatermarks[Row]],
    processingTimeProvider: ProcessingTimeService,
    autoWatermarkInterval: Long)
  extends PulsarFetcher(
    sourceContext,
    seedTopicsWithInitialOffsets,
    watermarksPeriodic,
    watermarksPunctuated,
    processingTimeProvider,
    autoWatermarkInterval,
    getClass.getClassLoader, null, "", null, null, null, 0, null) {

  @volatile var isRunning: Boolean = true

  override def runFetchLoop(): Unit = {
    while (isRunning) {
      Thread.sleep(10L)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  override def doCommitOffsetToPulsar(
    offset: Map[String, MessageId],
    offsetCommitCallback: PulsarCommitCallback): Unit = {}
}

class TestingFlinkPulsarSource(discoverer: PulsarMetadataReader, discoveryIntervalMs: Long)
  extends FlinkPulsarSource(FlinkPulsarSourceTest.dummyProperties) with MockitoSugar {

  override protected def createFetcher(
    sourceContext: SourceContext[Row],
    seedTopicsWithInitialOffsets: Map[String, MessageId],
    watermarksPeriodic: SerializedValue[AssignerWithPeriodicWatermarks[Row]],
    watermarksPunctuated: SerializedValue[AssignerWithPunctuatedWatermarks[Row]],
    processingTimeProvider: ProcessingTimeService,
    autoWatermarkInterval: Long, userCodeClassLoader: ClassLoader,
    streamingRuntime: StreamingRuntimeContext): PulsarFetcher = {

    new TestingFetcher(sourceContext,
      seedTopicsWithInitialOffsets,
      watermarksPeriodic,
      watermarksPunctuated,
      processingTimeProvider,
      autoWatermarkInterval)
  }

  override protected def createTopicDiscoverer(): PulsarMetadataReader = {
    discoverer
  }
}

class TestingListState[T] extends ListState[T] {
  private val list = new util.ArrayList[T]
  private var clearCalled = false

  override def clear(): Unit = {
    list.clear()
    clearCalled = true
  }

  override def get: java.lang.Iterable[T] = list

  override def add(value: T): Unit = {
    Preconditions.checkNotNull(value, "You cannot add null to a ListState.")
    list.add(value)
  }

  def getList: util.List[T] = list

  def isClearCalled = clearCalled

  override def update(values: util.List[T]): Unit = {
    clear()
    addAll(values)
  }

  override def addAll(values: util.List[T]): Unit = {
    if (values != null) {
      values.asScala.foreach { v =>
        Preconditions.checkNotNull(v, "You cannot add null to a ListState.")
      }
      list.addAll(values)
    }
  }
}

class MockFetcher(
  states: Seq[util.Map[String, MessageId]])
  extends PulsarFetcher(
    new TestSourceContext[Row](),
    Map.empty[String, MessageId],
    null,
    null,
    new TestProcessingTimeService(),
    0,
    classOf[MockFetcher].getClassLoader, null, "", null, null, null, 0, null) {

  val runLatch = new OneShotLatch()
  val stopLatch = new OneShotLatch()

  private val stateSnapshotsToReturn = new util.ArrayDeque[util.Map[String, MessageId]]
  stateSnapshotsToReturn.addAll(states.asJava)

  private var lastCommittedOffsets: Map[String, MessageId] = null
  private var commitCount: Int = 0

  override def doCommitOffsetToPulsar(
    offset: Map[String, MessageId],
    offsetCommitCallback: PulsarCommitCallback): Unit = {
    this.lastCommittedOffsets = offset
    this.commitCount += 1
    offsetCommitCallback.onSuccess()
  }

  override def runFetchLoop(): Unit = {
    runLatch.trigger()
    stopLatch.await()
  }

  override def snapshotCurrentState(): Map[String, MessageId] = {
    Preconditions.checkState(!stateSnapshotsToReturn.isEmpty)
    stateSnapshotsToReturn.poll().asScala.toMap
  }

  override def cancel(): Unit = {
    stopLatch.trigger()
  }

  def waitUntilRun(): Unit = {
    runLatch.await()
  }

  def getAndClearLastCommittedOffsets() = {
    val off = this.lastCommittedOffsets
    this.lastCommittedOffsets = null
    off
  }

  def getCommitCount(): Int = commitCount
}

class MockFunctionInitializationContext(
  isRestored: Boolean,
  operatorStateStore: OperatorStateStore) extends FunctionInitializationContext {

  override def getOperatorStateStore: OperatorStateStore = operatorStateStore

  override def getKeyedStateStore = throw new UnsupportedOperationException

  override def isRestored: Boolean = this.isRestored
}

class MockOperatorStateStore(mockRestoredUnionListState: ListState[_]) extends OperatorStateStore {

  override def getUnionListState[S](stateDescriptor: ListStateDescriptor[S]): ListState[S] = {
    mockRestoredUnionListState.asInstanceOf[ListState[S]]
  }

  override def getSerializableListState[T <: io.Serializable](s: String): ListState[T] = {
    new TestingListState[T]()
  }

  override def getOperatorState[S](stateDescriptor: ListStateDescriptor[S]) = {
    throw new UnsupportedOperationException
  }

  override def getBroadcastState[K, V](
    stateDescriptor: MapStateDescriptor[K, V]) = {
    throw new UnsupportedOperationException
  }

  override def getListState[S](stateDescriptor: ListStateDescriptor[S]) = {
    throw new UnsupportedOperationException
  }

  override def getRegisteredStateNames = {
    throw new UnsupportedOperationException
  }

  override def getRegisteredBroadcastStateNames = {
    throw new UnsupportedOperationException
  }
}

class DummyFlinkPulsarSource(
  testFetcherSupplier: SupplierWithException[PulsarFetcher, Exception],
  discoverer: PulsarMetadataReader,
  discoveryIntervalMs: Int)
  extends FlinkPulsarSource(FlinkPulsarSourceTest.dummyProperties) {

  def this() = this(
    new SupplierWithException[PulsarFetcher, Exception] {
      override def get() = mock(classOf[PulsarFetcher])
    },
    mock(classOf[PulsarMetadataReader]), -1)

  def this(discoverer: PulsarMetadataReader) = this(
    new SupplierWithException[PulsarFetcher, Exception] {
      override def get(): PulsarFetcher = mock(classOf[PulsarFetcher])
    },
    discoverer, -1)

  def this(fetcher: PulsarFetcher, discoverer: PulsarMetadataReader) = this(
    new SupplierWithException[PulsarFetcher, Exception] {
      override def get(): PulsarFetcher = fetcher
    }, discoverer, -1)

  override protected def createFetcher(
    sourceContext: SourceContext[Row],
    seedTopicsWithInitialOffsets: Map[String, MessageId],
    watermarksPeriodic: SerializedValue[AssignerWithPeriodicWatermarks[Row]],
    watermarksPunctuated: SerializedValue[AssignerWithPunctuatedWatermarks[Row]],
    processingTimeProvider: ProcessingTimeService,
    autoWatermarkInterval: Long, userCodeClassLoader: ClassLoader,
    streamingRuntime: StreamingRuntimeContext): PulsarFetcher = {
    testFetcherSupplier.get()
  }

  override protected def createTopicDiscoverer(): PulsarMetadataReader = {
    discoverer
  }
}

object FlinkPulsarSourceTest {
  val dummyProperties = new Properties()
  dummyProperties.setProperty("service.url", "a")
  dummyProperties.setProperty("admin.url", "b")
  dummyProperties.setProperty("topic", "c")
}
