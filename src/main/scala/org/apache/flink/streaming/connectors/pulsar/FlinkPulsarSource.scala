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
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.collection.mutable

import org.apache.commons.collections.map.LinkedMap

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.ClosureCleaner
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.connectors.pulsar.internal.{CachedPulsarClient, ClosedException, LatestOffset, Logging, PulsarCommitCallback, PulsarFetcher, PulsarMetadataReader, SourceSinkUtils, Utils}
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
import org.apache.flink.types.Row
import org.apache.flink.util.{ExceptionUtils, SerializedValue}
import org.apache.flink.util.Preconditions.checkNotNull

import org.apache.pulsar.client.api.MessageId

class FlinkPulsarSource(val parameters: Properties)
  extends RichParallelSourceFunction[Row]
  with ResultTypeQueryable[Row]
  with CheckpointListener
  with CheckpointedFunction
  with Logging {

  import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils._
  import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions._
  import org.apache.flink.streaming.connectors.pulsar.internal.metrics.PulsarSourceMetrics._

  // ------------------------------------------------------------------------
  //  configuration state, set on the client relevant for all subtasks
  // ------------------------------------------------------------------------
  val caseInsensitiveParams =
    validateStreamSourceOptions(parameters.asScala.toMap)

  val (clientConf, readerConf, serviceUrl, adminUrl) =
    prepareConfForReader(parameters.asScala.toMap)

  val (useExternalSub, externalSubName, removeSubOnStop) =
    prepareSubscriptionConf(parameters.asScala.toMap)

  val discoveryIntervalMs =
    getPartitionDiscoveryIntervalInMillis(caseInsensitiveParams)

  CachedPulsarClient.setCacheSize(clientCacheSize(caseInsensitiveParams))

  /**
   * Flag indicating whether or not metrics should be exposed.
   * If {@code true}, offset metrics (e.g. current offset, committed offset) will be registered.
   */
  val useMetrics = enableMetrics(caseInsensitiveParams)

  var ownedTopicStarts: mutable.HashMap[String, MessageId] = null

  var periodicWatermarkAssigner:
    SerializedValue[AssignerWithPeriodicWatermarks[Row]] = null

  var punctuatedWatermarkAssigner:
    SerializedValue[AssignerWithPunctuatedWatermarks[Row]] = null

  // ------------------------------------------------------------------------
  //  runtime state (used individually by each parallel subtask)
  // ------------------------------------------------------------------------

  @transient private lazy val taskIndex = getRuntimeContext.getIndexOfThisSubtask

  @transient private lazy val numParallelTasks = getRuntimeContext.getNumberOfParallelSubtasks

  /** Data for pending but uncommitted offsets. */
  private val pendingOffsetsToCommit = new LinkedMap()

  /** Fetcher implements Pulsar reads. */
  @transient @volatile private var pulsarFetcher: PulsarFetcher = null

  @transient @volatile private var metadataReader: PulsarMetadataReader = null

  /**
   * The offsets to restore to, if the reader restores state from a checkpoint.
   *
   * <p>This map will be populated by the
   * {@link #initializeState(FunctionInitializationContext)} method.
   *
   * <p>Using a sorted map as the ordering is important when using restored state
   * to seed the partition discoverer.
   */
  @transient @volatile private var restoredState: TreeMap[String, MessageId] = null

  /** Accessor for state in the operator state backend. */
  @transient private var unionOffsetStates: ListState[Tuple2[String, MessageId]] = null

  /** Discovery loop, executed in a separate thread. */
  @transient @volatile private var discoveryLoopThread: Thread = null

  /** Flag indicating whether the reader is still running. */
  @volatile private var running: Boolean = true

  /** Counter for successful Pulsar offset commits. */
  @transient var successfulCommits: Counter = null

  /** Counter for failed Pulsar offset commits. */
  @transient var failedCommits: Counter = null

  /** Callback interface that will be invoked upon async pulsar commit completion. */
  private var offsetCommitCallback: PulsarCommitCallback = null

  @transient @volatile lazy val inferredSchema = Utils.tryWithResource(
    PulsarMetadataReader(adminUrl, clientConf, "", caseInsensitiveParams)) { reader =>
    val topics = reader.getTopics()
    reader.getSchema(topics)
  }


  // ------------------------------------------------------------------------
  //  Configuration
  // ------------------------------------------------------------------------

  /**
   * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks
   * in a punctuated manner. The watermark extractor will run per Pulsar partition,
   * watermarks will be merged across partitions in the same way as in the Flink runtime,
   * when streams are merged.
   *
   * <p>When a subtask of a FlinkPulsarSource source reads multiple Pulsar partitions,
   * the streams from the partitions are unioned in a "first come first serve" fashion.
   * Per-partition characteristics are usually lost that way.
   * For example, if the timestamps are strictly ascending per Pulsar partition,
   * they will not be strictly ascending in the resulting Flink DataStream, if the
   * parallel source subtask reads more that one partition.
   *
   * <p>Running timestamp extractors / watermark generators directly inside the Pulsar source,
   * per Pulsar partition, allows users to let them exploit the per-partition characteristics.
   *
   * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
   * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
   *
   * @param assigner The timestamp assigner / watermark generator to use.
   * @return The reader object, to allow function chaining.
   */
  def assignTimestampsAndWatermarks(
      assigner: AssignerWithPunctuatedWatermarks[Row]): FlinkPulsarSource = {
    checkNotNull(assigner)
    if (this.periodicWatermarkAssigner != null) {
      throw new IllegalStateException("A periodic watermark emitter has already been set.")
    }
    try {
      ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true)
      this.punctuatedWatermarkAssigner =
        new SerializedValue[AssignerWithPunctuatedWatermarks[Row]](assigner)
      return this
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException("The given assigner is not serializable", e)
    }
  }

  /**
   * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks
   * in a punctuated manner. The watermark extractor will run per Pulsar partition,
   * watermarks will be merged across partitions in the same way as in the Flink runtime,
   * when streams are merged.
   *
   * <p>When a subtask of a FlinkPulsarSource source reads multiple Pulsar partitions,
   * the streams from the partitions are unioned in a "first come first serve" fashion.
   * Per-partition characteristics are usually lost that way.
   * For example, if the timestamps are strictly ascending per Pulsar partition,
   * they will not be strictly ascending in the resulting Flink DataStream,
   * if the parallel source subtask reads more that one partition.
   *
   * <p>Running timestamp extractors / watermark generators directly inside the Pulsar source,
   * per Pulsar partition, allows users to let them exploit the per-partition characteristics.
   *
   * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
   * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
   *
   * @param assigner The timestamp assigner / watermark generator to use.
   * @return The reader object, to allow function chaining.
   */
  def assignTimestampsAndWatermarks(
      assigner: AssignerWithPeriodicWatermarks[Row]): FlinkPulsarSource = {
    checkNotNull(assigner)
    if (this.punctuatedWatermarkAssigner != null) {
      throw new IllegalStateException("A punctuated watermark emitter has already been set.")
    }
    try {
      ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true)
      this.periodicWatermarkAssigner =
        new SerializedValue[AssignerWithPeriodicWatermarks[Row]](assigner)
      this
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException("The given assigner is not serializable", e)
    }
  }


  // ------------------------------------------------------------------------
  //  Work methods
  // ------------------------------------------------------------------------

  override def open(parameters: Configuration): Unit = {
    this.metadataReader = createMetadataReader()

    ownedTopicStarts = new mutable.HashMap[String, MessageId]()
    val allTopics = metadataReader.discoverTopicsChange()

    if (restoredState != null) {
      allTopics.filter(!restoredState.contains(_)).foreach { tp =>
        restoredState += (tp -> MessageId.earliest)
      }

      restoredState
        .filter(s => SourceSinkUtils.belongsTo(s._1, numParallelTasks, taskIndex))
        .foreach { case (tp, mid) =>
          ownedTopicStarts.put(tp, mid)
        }

      val goneTopics =
        restoredState.keys.toSet.diff(allTopics)
          .filter(SourceSinkUtils.belongsTo(_, numParallelTasks, taskIndex))

      goneTopics.foreach { tp =>
        logWarning(s"$tp is removed from subscription since " +
          "it no longer matches with topics settings.")
        ownedTopicStarts.remove(tp)
      }
      logInfo(s"Source $taskIndex will start reading" +
        s" ${ownedTopicStarts.size} topics in restored state $ownedTopicStarts.")

    } else {
      val allTopicOffsets = offsetForEachTopic(
        allTopics,
        caseInsensitiveParams,
        STARTING_OFFSETS_OPTION_KEY,
        LatestOffset).topicOffsets

      val ownOffsetsFromParam = allTopicOffsets
        .filter(o => SourceSinkUtils.belongsTo(o._1, numParallelTasks, taskIndex))

      val ownOffsets = if (useExternalSub) {
        metadataReader.getStartPositionFromSubscription(externalSubName, ownOffsetsFromParam)
      } else {
        ownOffsetsFromParam
      }

      ownedTopicStarts ++= ownOffsets

      if (ownedTopicStarts.isEmpty) {
        logInfo(s"Source $taskIndex initially has no topics to read from.")
      } else {
        logInfo(s"Source $taskIndex will start reading" +
          s" ${ownedTopicStarts.size} topics from initialized positions.")
      }
    }

  }

  override def getProducedType: TypeInformation[Row] = {
    LegacyTypeInfoDataTypeConverter
      .toLegacyTypeInfo(inferredSchema)
      .asInstanceOf[TypeInformation[Row]]
  }

  override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit = {
    if (ownedTopicStarts == null) {
      throw new Exception("The partitions were not set for the source")
    }

    this.successfulCommits =
      this.getRuntimeContext.getMetricGroup.counter(COMMITS_SUCCEEDED_METRICS_COUNTER)
    this.failedCommits =
      this.getRuntimeContext.getMetricGroup.counter(COMMITS_FAILED_METRICS_COUNTER)

    this.offsetCommitCallback = new PulsarCommitCallback {

      override def onSuccess(): Unit = {
        successfulCommits.inc()
      }

      override def onException(throwable: Throwable): Unit = {
        logWarning(s"source $taskIndex failed commit by $throwable")
        failedCommits.inc()
      }
    }

    if (ownedTopicStarts.isEmpty) {
      sourceContext.markAsTemporarilyIdle()
    }

    logInfo(s"Source $taskIndex creating fetcher with offsets $ownedTopicStarts")

    // from this point forward:
    //   - 'snapshotState' will draw offsets from the fetcher,
    //     instead of being built from `subscribedPartitionsToStartOffsets`
    //   - 'notifyCheckpointComplete' will start to do work (i.e. commit offsets to
    //     Pulsar through the fetcher, if configured to do so)

    val streamingRuntime = getRuntimeContext.asInstanceOf[StreamingRuntimeContext]

    this.pulsarFetcher = createFetcher(
      sourceContext,
      ownedTopicStarts.toMap,
      periodicWatermarkAssigner,
      punctuatedWatermarkAssigner,
      streamingRuntime.getProcessingTimeService,
      streamingRuntime.getExecutionConfig.getAutoWatermarkInterval,
      getRuntimeContext.getUserCodeClassLoader,
      streamingRuntime)

    if (!running) {
      return
    }

    if (discoveryIntervalMs < 0) {
      pulsarFetcher.runFetchLoop()
    } else {
      runWithTopicsDiscovery()
    }
  }

  def runWithTopicsDiscovery(): Unit = {
    val discoveryLoopErrorRef = new AtomicReference[Exception]()
    createAndStartDiscoveryLoop(discoveryLoopErrorRef)

    pulsarFetcher.runFetchLoop()

    joinDiscoveryLoopThread()

    val discoveryLoopError = discoveryLoopErrorRef.get()
    if (discoveryLoopError != null) {
      throw new RuntimeException(discoveryLoopError)
    }
  }

  def createAndStartDiscoveryLoop(discoveryLoopErrorRef: AtomicReference[Exception]): Unit = {
    discoveryLoopThread = new Thread(
      new Runnable {
        override def run(): Unit = {
          try {

            while (running) {

              val added = metadataReader.discoverTopicsChange()

              if (running && !added.isEmpty) {
                pulsarFetcher.addDiscoveredTopics(added)
              }

              if (running && discoveryIntervalMs != 0) {
                Thread.sleep(discoveryIntervalMs)
              }
            }

          } catch {
            case e: ClosedException => // break out while and do nothing
            case e: InterruptedException => // break out while and do nothing
            case e: Exception =>
              discoveryLoopErrorRef.set(e)
          } finally {
            if (running) {
              // calling cancel will also let the fetcher loop escape
              // (if not running, cancel() was already called)
              cancel()
            }
          }
        }
      }, s"Pulsar Topic Discovery for source $taskIndex")
    discoveryLoopThread.start()
  }

  def joinDiscoveryLoopThread(): Unit = {
    if (discoveryLoopThread != null) {
      discoveryLoopThread.join()
    }
  }

  override def cancel(): Unit = {
    running = false

    if (discoveryLoopThread != null) {
      discoveryLoopThread.interrupt()
    }

    if (pulsarFetcher != null) {
      pulsarFetcher.cancel()
    }
  }

  override def close(): Unit = {
    cancel()

    joinDiscoveryLoopThread()

    var exception: Exception = null

    if (metadataReader != null) {
      try {
        metadataReader.close()
      } catch {
        case e: Exception =>
          exception = e
      }
    }

    try {
      super.close()
    } catch {
      case e: Exception =>
        exception = ExceptionUtils.firstOrSuppressed(e, exception)
    }

    if (exception != null) {
      throw exception
    }
  }

  // ------------------------------------------------------------------------
  //  Checkpoint and restore
  // ------------------------------------------------------------------------

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateStore = context.getOperatorStateStore

    unionOffsetStates = stateStore.getUnionListState(
      new ListStateDescriptor[Tuple2[String, MessageId]](
        FlinkPulsarSource.OFFSETS_STATE_NAME,
        TypeInformation.of(new TypeHint[Tuple2[String, MessageId]]{})))

    if (context.isRestored) {
      restoredState = TreeMap.empty[String, MessageId]
      unionOffsetStates.get().asScala.foreach { t2 =>
        restoredState += (t2.f0 -> t2.f1)
      }
      logInfo(s"Source subtask ${getRuntimeContext.getIndexOfThisSubtask} " +
        s"restored state $restoredState.")
    } else {
      logInfo(s"Source subtask ${getRuntimeContext.getIndexOfThisSubtask} has no restore state.")
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    if (!running) {
      logDebug("snapshotState() called on closed source")
    } else {
      unionOffsetStates.clear()

      val fetcher = this.pulsarFetcher

      if (fetcher == null) {
        // the fetcher has not yet been initialized, which means we need to return the
        // originally restored offsets or the assigned partitions
        ownedTopicStarts.foreach { case (tp, mid) =>
          unionOffsetStates.add(Tuple2.of(tp, mid))
        }
        pendingOffsetsToCommit.put(context.getCheckpointId, restoredState)
      } else {
        val currentOffsets = fetcher.snapshotCurrentState()
        pendingOffsetsToCommit.put(context.getCheckpointId, currentOffsets)
        currentOffsets.foreach { case (tp, mid) =>
          unionOffsetStates.add(Tuple2.of(tp, mid))
        }
        while (pendingOffsetsToCommit.size() > FlinkPulsarSource.MAX_NUM_PENDING_CHECKPOINTS) {
          pendingOffsetsToCommit.remove(0)
        }
      }
    }
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    if (!running) {
      logInfo("notifyCheckpointComplete() called on closed source")
      return
    }

    val fetcher = this.pulsarFetcher

    if (fetcher == null) {
      logInfo("notifyCheckpointComplete() called on uninitialized source")
      return
    }

    logDebug(s"Source $taskIndex committing offsets to Pulsar for checkpoint $checkpointId")

    try {
      val posInMap = pendingOffsetsToCommit.indexOf(checkpointId)
      if (posInMap == -1) {
        logWarning(
          s"Source $taskIndex received confirmation for unknown checkpoint id $checkpointId")
        return
      }

      val offset = pendingOffsetsToCommit.remove(posInMap).asInstanceOf[Map[String, MessageId]]

      // remove older checkpoints in map
      var i = 0
      while (i < posInMap) {
        pendingOffsetsToCommit.remove(0)
        i += 1
      }

      if (offset == null || offset.size == 0) {
        logDebug(s"Source $taskIndex has empty checkpoint state")
        return
      }
      fetcher.commitOffsetToPulsar(offset, offsetCommitCallback)
    } catch {
      case e: Exception =>
        if (running) {
          throw e
        }
    }
  }

  protected def createFetcher(
    sourceContext: SourceFunction.SourceContext[Row],
    seedTopicsWithInitialOffsets: Map[String, MessageId],
    watermarksPeriodic: SerializedValue[AssignerWithPeriodicWatermarks[Row]],
    watermarksPunctuated: SerializedValue[AssignerWithPunctuatedWatermarks[Row]],
    processingTimeProvider: ProcessingTimeService,
    autoWatermarkInterval: Long,
    userCodeClassLoader: ClassLoader,
    streamingRuntime: StreamingRuntimeContext) = {
    new PulsarFetcher(
      sourceContext,
      seedTopicsWithInitialOffsets,
      watermarksPeriodic,
      watermarksPunctuated,
      processingTimeProvider,
      autoWatermarkInterval,
      userCodeClassLoader,
      streamingRuntime,
      adminUrl,
      clientConf,
      readerConf,
      metadataReader,
      pollTimeoutMs(caseInsensitiveParams),
      SourceSinkUtils.jsonOptions)
  }

  @transient lazy val subscriptionPrefix: String =
    if (useExternalSub) externalSubName else s"flink-pulsar-${UUID.randomUUID()}"

  protected def createMetadataReader() = {
    new PulsarMetadataReader(
      adminUrl,
      clientConf,
      subscriptionPrefix,
      caseInsensitiveParams,
      taskIndex,
      numParallelTasks,
      useExternalSub,
      removeSubOnStop)
  }

  def getPendingOffsetsToCommit(): LinkedMap = {
    pendingOffsetsToCommit
  }
}

object FlinkPulsarSource {

  /** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks. */
  val MAX_NUM_PENDING_CHECKPOINTS = 100

  /** State name of the reader's partition offset states. */
  val OFFSETS_STATE_NAME = "topic.partition.offset.states"
}
