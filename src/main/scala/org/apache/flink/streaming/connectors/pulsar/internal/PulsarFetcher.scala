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
package org.apache.flink.streaming.connectors.pulsar.internal

import java.{util => ju}
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Predicate

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.pulsar.{JSONOptionsInRead, Logging, PulsarMetadataReader}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.pulsar.PulsarTopicStateWithPunctuatedWatermarks
import org.apache.flink.streaming.runtime.tasks.{ProcessingTimeCallback, ProcessingTimeService}
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.util.SerializedValue

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.common.schema.SchemaInfo

sealed trait TimestampWatermarkMode
object NoWatermark extends TimestampWatermarkMode
object PeriodicWatermark extends TimestampWatermarkMode
object PunctuatedWatermark extends TimestampWatermarkMode

class PulsarFetcher(
    sourceContext: SourceContext[GenericRow],
    seedTopicsWithInitialOffsets: Map[String, MessageId],
    watermarksPeriodic: SerializedValue[AssignerWithPeriodicWatermarks[GenericRow]],
    watermarksPunctuated: SerializedValue[AssignerWithPunctuatedWatermarks[GenericRow]],
    processingTimeProvider: ProcessingTimeService,
    autoWatermarkInterval: Long,
    userCodeClassLoader: ClassLoader,
    runtimeContext: StreamingRuntimeContext,
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    metadataReader: PulsarMetadataReader,
    pollTimeoutMs: Int,
    jsonOptions: JSONOptionsInRead) extends Logging {

  @volatile var running: Boolean = true

  /** Only relevant for punctuated watermarks: The current cross partition watermark. */
  private var maxWatermarkSoFar = Long.MinValue

  val topicToThread: mutable.Map[String, Thread] = mutable.HashMap.empty[String, Thread]

  private val watermarkMode: TimestampWatermarkMode =
    if (watermarksPeriodic == null) {
      if (watermarksPunctuated == null) {
        NoWatermark
      } else {
        PunctuatedWatermark
      }
    } else {
      if (watermarksPunctuated == null) {
        PeriodicWatermark
      } else {
        throw new IllegalArgumentException("Cannot have both periodic and punctuated watermarks")
      }
    }

  private val checkpointLock: Object = sourceContext.getCheckpointLock

  val pulsarSchema: SchemaInfo =
    metadataReader.getPulsarSchema(seedTopicsWithInitialOffsets.keySet.toSeq)

  private val subscribedTopicStates: ju.List[PulsarTopicState] =
    createTopicStateHolders(seedTopicsWithInitialOffsets,
      watermarkMode, watermarksPeriodic, watermarksPunctuated, userCodeClassLoader)

  private val unassignedPartitionsQueue = new ClosableBlockingQueue[PulsarTopicState]()
  subscribedTopicStates.asScala.foreach(unassignedPartitionsQueue.add(_))

  // if we have periodic watermarks, kick off the interval scheduler
  watermarkMode match {
    case PeriodicWatermark =>
      val periodicWatermarkEmitter =
        new PeriodicWatermarkEmitter(
          subscribedTopicStates,
          sourceContext,
          processingTimeProvider,
          autoWatermarkInterval)
      periodicWatermarkEmitter.start()
    case _ => // do nothing
  }

  implicit def toJavaPredicate[A](f: (A) => Boolean): Predicate[A] = new Predicate[A] {
    override def test(a: A): Boolean = f(a)
  }

  def runFetchLoop(): Unit = {

    val topicToThread = mutable.HashMap.empty[String, ReaderThread]

    val exceptionProxy = new ExceptionProxy(Thread.currentThread())

    try {

      while (running) {
        // re-throw any exception from the concurrent fetcher threads
        exceptionProxy.checkAndThrowException()

        // wait for max 5 seconds trying to get partitions to assign
        // if threads shut down, this poll returns earlier, because the threads inject the
        // special marker into the queue
        val topicsToAssign = unassignedPartitionsQueue.getBatchBlocking(5000)
        // if there are more markers, remove them all
        topicsToAssign.removeIf(PoisonState.equals(_))

        if (!topicsToAssign.isEmpty) {

          if (!running) {
            throw BreakingException
          }

          topicToThread ++=
            createAndStartReaderThread(topicsToAssign, exceptionProxy)
        } else {
          // there were no partitions to assign. Check if any consumer threads shut down.
          // we get into this section of the code, if either the poll timed out, or the
          // blocking poll was woken up by the marker element

          val deadThreads = topicToThread.filter { case (tp, thread) =>
            !thread.isRunning
          }

          deadThreads.foreach { case (tp, _) => topicToThread.remove(tp) }
        }

        if (topicToThread.size == 0 && unassignedPartitionsQueue.isEmpty) {
          if (unassignedPartitionsQueue.close()) {
            logInfo("All reader threads are finished, " +
              "there are no more unassigned partitions. Stopping fetcher")
            throw BreakingException
          }
        }
      }
    } catch {
      case BreakingException => // do nothing
      case e: InterruptedException =>
        // this may be thrown because an exception on one of the concurrent fetcher threads
        // woke this thread up. make sure we throw the root exception instead in that case
        exceptionProxy.checkAndThrowException()

        // no other root exception, throw the interrupted exception
        throw e
    } finally {
      running = false

      // clear the interruption flag
      // this allows the joining on reader threads (on best effort) to happen in
      // case the initial interrupt already
      Thread.interrupted()

      // make sure that in any case (completion, abort, error), all spawned threads are stopped
      try {
        var runningThreads = 0
        do { // check whether threads are alive and cancel them
          runningThreads = 0

          topicToThread.retain { case (_, t) => t.isAlive }
          topicToThread.foreach { case (_, t) =>
              t.cancel()
              runningThreads += 1
          }

          if (runningThreads > 0) {
            topicToThread.foreach { case (_, t) =>
              t.join(500 / runningThreads + 1)
            }
          }

        } while (runningThreads > 0)
      } catch {
        case _: InterruptedException =>
          // waiting for the thread shutdown apparently got interrupted
          // restore interrupted state and continue
          Thread.currentThread.interrupt()
        case t: Throwable =>
          // we catch all here to preserve the original exception
          logError("Exception while shutting down reader threads", t)
      }
    }
  }

  // start from earliest for new topics.
  def addDiscoveredTopics(newTopics: Set[String]): Unit = {
    val newStates = createTopicStateHolders(
      newTopics.map((_, MessageId.earliest)).toMap,
      watermarkMode,
      watermarksPeriodic,
      watermarksPunctuated,
      userCodeClassLoader)

    newStates.asScala.foreach { state =>
      subscribedTopicStates.add(state)
      unassignedPartitionsQueue.add(state)
    }
  }

  def snapshotCurrentState(): Map[String, MessageId] = {
    assert(Thread.holdsLock(checkpointLock))

    val states = mutable.HashMap.empty[String, MessageId]
    subscribedTopicStates.asScala.foreach { state =>
      states.put(state.topic, state.offset)
    }
    states.toMap
  }

  def cancel(): Unit = {
    // signal the main thread to exit
    running = false

    val topics = subscribedTopicStates.asScala.map(_.topic).toSeq
    metadataReader.removeCursor(topics)
    metadataReader.close()

    // make sure the main thread wakes up soon
    unassignedPartitionsQueue.addIfOpen(PoisonState)
  }

  def createAndStartReaderThread(
    topicStates: ju.List[PulsarTopicState],
    exceptionProxy: ExceptionProxy): Map[String, ReaderThread] = {

    val startingOffsets = topicStates.asScala.map(state => (state.topic, state.offset)).toMap
    metadataReader.setupCursor(startingOffsets)

    topicStates.asScala.map { state =>
      val readerT = new ReaderThread(
        this,
        state,
        pulsarSchema,
        clientConf,
        readerConf,
        pollTimeoutMs,
        jsonOptions,
        exceptionProxy)

      readerT.setName(
        s"Pulsar Reader for ${state.topic} in task ${runtimeContext.getTaskName}")
      readerT.setDaemon(true)
      readerT.start()

      logInfo(s"Starting thread ${readerT.getName}")
      (state.topic, readerT)
    }.toMap
  }

  def createTopicStateHolders(
      seedTopicsWithInitialOffsets: Map[String, MessageId],
      watermarkMode: TimestampWatermarkMode,
      watermarksPeriodic: SerializedValue[AssignerWithPeriodicWatermarks[GenericRow]],
      watermarksPunctuated: SerializedValue[AssignerWithPunctuatedWatermarks[GenericRow]],
      userCodeClassLoader: ClassLoader): ju.List[PulsarTopicState] = {

    val topicStates = new CopyOnWriteArrayList[PulsarTopicState]()
    watermarkMode match {
      case NoWatermark =>
        seedTopicsWithInitialOffsets.foreach { case (topic, mid) =>
          val state = new PulsarTopicState(topic)
          state.offset = mid
          topicStates.add(state)
        }
      case PeriodicWatermark =>
        seedTopicsWithInitialOffsets.foreach { case (topic, mid) =>
          val state = new PulsarTopicStateWithPeriodicWatermarks(
            topic,
            watermarksPeriodic.deserializeValue(userCodeClassLoader))
          state.offset = mid
          topicStates.add(state)
        }
      case PunctuatedWatermark =>
        seedTopicsWithInitialOffsets.foreach { case (topic, mid) =>
          val state = new PulsarTopicStateWithPunctuatedWatermarks(
            topic,
            watermarksPunctuated.deserializeValue(userCodeClassLoader))
          state.offset = mid
          topicStates.add(state)
        }
    }
    topicStates
  }

  def commitOffsetToPulsar(offset: Map[String, MessageId]): Unit = {
    metadataReader.commitCursorToOffset(offset)
  }


  // ------------------------------------------------------------------------
  //  emitting records
  // ------------------------------------------------------------------------

  /**
   * Emits a record without attaching an existing timestamp to it.
   *
   * <p>Implementation Note: This method is kept brief to be JIT inlining friendly.
   * That makes the fast path efficient, the extended paths are called as separate methods.
   *
   * @param record         The record to emit
   * @param topicState     The state of the Pulsar topic from which the record was fetched
   * @param offset         The offset of the record
   */
  def emitRecord(
      record: GenericRow,
      topicState: PulsarTopicState,
      offset: MessageId): Unit = {

    if (record != null) {
      watermarkMode match {
        case NoWatermark =>
          checkpointLock.synchronized {
            sourceContext.collect(record)
            topicState.offset = offset
          }
        case PeriodicWatermark =>
          emitRecordWithTimestampAndPeriodicWatermark(record, topicState, offset, Long.MinValue)
        case PunctuatedWatermark =>
          emitRecordWithTimestampAndPunctuatedWatermark(record, topicState, offset, Long.MinValue)
      }
    } else {
      checkpointLock.synchronized {
        topicState.offset = offset
      }
    }
  }

  /**
   * Record emission, if a timestamp will be attached from an assigner that is
   * also a periodic watermark generator.
   */
  private def emitRecordWithTimestampAndPeriodicWatermark(
      record: GenericRow,
      topicState: PulsarTopicState,
      offset: MessageId,
      eventTimestamp: Long): Unit = {

    val periodicState = topicState.asInstanceOf[PulsarTopicStateWithPeriodicWatermarks]

    var timestamp: Long = 0

    periodicState.synchronized {
      timestamp = periodicState.getTimestampForRow(record, eventTimestamp)
    }

    checkpointLock.synchronized {
      sourceContext.collectWithTimestamp(record, timestamp)
      topicState.offset = offset
    }
  }

  /**
   * Record emission, if a timestamp will be attached from an assigner that is
   * also a punctuated watermark generator.
   */
  private def emitRecordWithTimestampAndPunctuatedWatermark(
      record: GenericRow,
      topicState: PulsarTopicState,
      offset: MessageId,
      eventTimestamp: Long): Unit = {

    val punctuatedState = topicState.asInstanceOf[PulsarTopicStateWithPunctuatedWatermarks]
    val timestamp = punctuatedState.getTimestampForRow(record, eventTimestamp)
    val newWM = punctuatedState.checkAndGetNewWatermark(record, timestamp)

    checkpointLock.synchronized {
      sourceContext.collectWithTimestamp(record, timestamp)
      topicState.offset = offset
    }

    if (newWM != null) {
      updateMinPunctuatedWatermark(newWM)
    }
  }

  /**
   * Checks whether a new per-partition watermark is also a new cross-partition watermark.
   */
  private def updateMinPunctuatedWatermark(nextWatermark: Watermark): Unit = {
    if (nextWatermark.getTimestamp > maxWatermarkSoFar) {
      var newMin = Long.MaxValue
      subscribedTopicStates.asScala.foreach { state =>
        val puncState = state.asInstanceOf[PulsarTopicStateWithPunctuatedWatermarks]
        newMin = Math.min(newMin, puncState.getCurrentPartitionWatermark)
      }

      if (newMin > maxWatermarkSoFar) {
        checkpointLock.synchronized {
          if (newMin > maxWatermarkSoFar) {
            maxWatermarkSoFar = newMin
            sourceContext.emitWatermark(new Watermark(newMin))
          }
        }
      }
    }
  }
}

/**
 * The periodic watermark emitter. In its given interval, it checks all partitions for
 * the current event time watermark, and possibly emits the next watermark.
 */
class PeriodicWatermarkEmitter(
    allPartitions: ju.List[PulsarTopicState],
    emitter: SourceFunction.SourceContext[_],
    timerService: ProcessingTimeService,
    interval: Long)
  extends ProcessingTimeCallback {

  var lastWatermarkTimestamp: Long = Long.MinValue

  def start(): Unit = {
    timerService.registerTimer(timerService.getCurrentProcessingTime + interval, this)
  }

  override def onProcessingTime(timestamp: Long): Unit = {

    var minAcrossAll = Long.MaxValue
    var isEffectiveMinAggregation = false

    allPartitions.asScala.foreach { state =>
      var curr: Long = 0
      state.synchronized {
        curr = state.asInstanceOf[PulsarTopicStateWithPeriodicWatermarks]
          .getCurrentWatermarkTimestamp()
      }
      minAcrossAll = Math.min(minAcrossAll, curr)
      isEffectiveMinAggregation = true
    }

    // emit next watermark, if there is one
    if (isEffectiveMinAggregation && minAcrossAll > lastWatermarkTimestamp) {
      lastWatermarkTimestamp = minAcrossAll
      emitter.emitWatermark(new Watermark(minAcrossAll))
    }

    // schedule the next watermark
    timerService.registerTimer(timerService.getCurrentProcessingTime + interval, this)
  }
}

case object BreakingException extends Exception
