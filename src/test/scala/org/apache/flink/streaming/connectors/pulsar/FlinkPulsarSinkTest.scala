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

import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

import org.apache.flink.core.testutils.{CheckedThread, MultiShotLatch}
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.operators.StreamSink
import org.apache.flink.streaming.connectors.pulsar.internals.PulsarFunSuite
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import org.apache.pulsar.client.api.{MessageId, Producer}
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito.{mock, times, verify, when}
import org.mockito.ArgumentMatchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.time.SpanSugar._
import org.scalatest.concurrent.TimeLimits

import scala.collection.mutable

class FlinkPulsarSinkTest extends PulsarFunSuite with MockitoSugar {

  test("instantiate should fail") {
    intercept[IllegalArgumentException] {
      new DummyFlinkPulsarSink(dummySchema(), new Properties())
    }
  }

  test("async error rethrown on checkpoint") {
    val sink = new DummyFlinkPulsarSink(dummySchema(), dummyProperties())

    val testHarness = new OneInputStreamOperatorTestHarness(new StreamSink[Row](sink))
    testHarness.open()
    testHarness.processElement(new StreamRecord(intRow(1)))

    sink.pendingCallbacks(0).accept(null, new Exception("async exception"))

    val ex = intercept[Throwable] {
      testHarness.snapshot(123L, 123L)
    }
    assert(ex.getCause.getMessage == "async exception")
  }

  test("async error rethrown on invoke") {
    val sink = new DummyFlinkPulsarSink(dummySchema(), dummyProperties())

    val testHarness = new OneInputStreamOperatorTestHarness(new StreamSink[Row](sink))
    testHarness.open()
    testHarness.processElement(new StreamRecord(intRow(1)))

    sink.pendingCallbacks(0).accept(null, new Exception("async exception"))

    val ex = intercept[Throwable] {
      testHarness.processElement(new StreamRecord(intRow(2)))
    }
    assert(ex.getMessage == "async exception")
  }

  test("async error rethrown on checkpoint after flush") {
    val sink = new DummyFlinkPulsarSink(dummySchema(), dummyProperties())
    val mockProducer = sink.getProducer("tp")

    val testHarness = new OneInputStreamOperatorTestHarness(new StreamSink[Row](sink))
    testHarness.open()
    testHarness.processElement(new StreamRecord(intRow(1)))
    testHarness.processElement(new StreamRecord(intRow(2)))
    testHarness.processElement(new StreamRecord(intRow(3)))

    verify(mockProducer, times(3)).newMessage()

    // only let the first callback succeed for now
    sink.pendingCallbacks(0).accept(null, null)

    val snapshotThread = new CheckedThread {
      override def go(): Unit = {
        // this should block at first,
        // since there are still two pending records that needs to be flushed
        testHarness.snapshot(123L, 123L)
      }
    }
    snapshotThread.start()

    sink.pendingCallbacks(1).accept(null, new Exception("failure on 2nd"))
    sink.pendingCallbacks(2).accept(null, null)

    val ex = intercept[Throwable] {
      snapshotThread.sync()
    }
    assert(ex.getCause.getMessage == "failure on 2nd")
  }

  test("at least once sink") {
    TimeLimits.failAfter(10 seconds) {
      val sink = new DummyFlinkPulsarSink(dummySchema(), dummyProperties())

      val mockProducer = sink.getProducer("tp")

      val testHarness = new OneInputStreamOperatorTestHarness(new StreamSink[Row](sink))
      testHarness.open()
      testHarness.processElement(new StreamRecord(intRow(1)))
      testHarness.processElement(new StreamRecord(intRow(2)))
      testHarness.processElement(new StreamRecord(intRow(3)))

      verify(mockProducer, times(3)).newMessage()
      assert(3 == sink.getPendingSize())

      val snapshotThread = new CheckedThread {
        override def go(): Unit = {
          // this should block until all records are flushed;
          // if the snapshot implementation returns before pending records are flushed
          testHarness.snapshot(123L, 123L)
        }
      }
      snapshotThread.start()

      sink.waitUntilFlushStarted()
      assert(snapshotThread.isAlive)

      sink.pendingCallbacks(0).accept(null, null)
      assert(snapshotThread.isAlive)
      assert(2 == sink.getPendingSize())

      sink.pendingCallbacks(1).accept(null, null)
      assert(snapshotThread.isAlive)
      assert(1 == sink.getPendingSize())

      sink.pendingCallbacks(2).accept(null, null)
      assert(snapshotThread.isAlive)
      assert(0 == sink.getPendingSize())

      snapshotThread.sync()

      testHarness.close()
    }
  }

  test("not wait for pending records if flushing disabled") {

    TimeLimits.failAfter(5 seconds) {
      val props = dummyProperties()
      props.setProperty("flushoncheckpoint", "false")
      val sink = new DummyFlinkPulsarSink(dummySchema(), props)

      val mockProducer = sink.getProducer("tp")
      val testHarness = new OneInputStreamOperatorTestHarness(new StreamSink[Row](sink))

      testHarness.open()
      testHarness.processElement(new StreamRecord(intRow(1)))

      verify(mockProducer, times(1)).newMessage()

      testHarness.snapshot(123L, 123L)

      testHarness.close()
    }
  }

  private def intRow(i: Int): Row = {
    val r = new Row(1)
    r.setField(0, i)
    r
  }

  def dummySchema(): DataType = {
    DataTypes.ROW(
      DataTypes.FIELD("1", DataTypes.INT))
  }

  def dummyProperties(): Properties = {
    val prop = new Properties()
    prop.setProperty("service.url", "a")
    prop.setProperty("admin.url", "b")
    prop.setProperty("topic", "t")
    prop.setProperty("failonwrite", "true")
    prop
  }
}

class DummyFlinkPulsarSink(
  schema: DataType,
  producerConfig: Properties)
  extends FlinkPulsarRowSink(schema, producerConfig) with MockitoSugar {

  val pendingCallbacks = mutable.ListBuffer.empty[BiConsumer[MessageId, Throwable]]
  val flushLatch = new MultiShotLatch()
  var isFlushed = false

  val mockProducer = mock[Producer[Any]]
  val mockMessageBuilder = mock[TypedMessageBuilderImpl[Any]]
  when(mockMessageBuilder.sendAsync()).thenAnswer(new Answer[CompletableFuture[MessageId]] {
    override def answer(
      invocation: InvocationOnMock): CompletableFuture[MessageId] = {
      val mockFuture = mock[CompletableFuture[MessageId]]

      when(mockFuture.whenComplete(any[BiConsumer[MessageId, Throwable]])).thenAnswer(new Answer[Any] {
        override def answer(invocation: InvocationOnMock): Any = {
          pendingCallbacks.append(invocation.getArgument(0))
          null
        }
      })

      mockFuture
    }
  })
  when(mockMessageBuilder.value(any())).thenReturn(mockMessageBuilder)
  when(mockMessageBuilder.keyBytes(any())).thenReturn(mockMessageBuilder)
  when(mockMessageBuilder.eventTime(any())).thenReturn(mockMessageBuilder)

  when(mockProducer.newMessage()).thenAnswer(new Answer[TypedMessageBuilderImpl[Any]]() {
    override def answer(
      invocation: InvocationOnMock): TypedMessageBuilderImpl[Any] = {
      mockMessageBuilder
    }
  })

  def getPendingSize(): Long = {
    if (doFlushOnCheckpoint) {
      numPendingRecords()
    } else {
      throw new UnsupportedOperationException("getPendingSize")
    }
  }

  override lazy val singleProducer = mockProducer

  override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
    isFlushed = false
    super.snapshotState(ctx)
    // if the snapshot implementation doesn't wait until
    // all pending records are flushed, we should fail the test
    if (doFlushOnCheckpoint && !isFlushed) {
      throw new RuntimeException("Flushing is enabled; snapshots should be blocked" +
        " until all pending records are flushed")
    }
  }

  def waitUntilFlushStarted(): Unit = {
    flushLatch.await()
  }

  override protected def producerFlush(): Unit = {
    flushLatch.trigger()
    // simply wait until the producer's pending records become zero.
    // This relies on the fact that the producer's Callback implementation
    // and pending records tracking logic is implemented correctly, otherwise
    // we will loop forever.
    while (numPendingRecords > 0) {
     try {
        Thread.sleep(10)
      } catch {
        case e: InterruptedException =>
          throw new RuntimeException("Unable to flush producer, task was interrupted")
      }
    }
    isFlushed = true
  }

  def numPendingRecords(): Long = {
    pendingRecordsLock.synchronized {
      pendingRecords
    }
  }
}
