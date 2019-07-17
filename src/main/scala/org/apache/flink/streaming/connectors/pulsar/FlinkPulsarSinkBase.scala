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

import java.{util => ju}
import java.io.Serializable
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.ClosureCleaner
import org.apache.flink.configuration.Configuration
import org.apache.flink.pulsar.{CachedPulsarClient, Logging, SchemaUtils}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.types.Row
import org.apache.flink.util.SerializableObject

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{MessageId, Producer, Schema}

/**
 * Flink Sink to produce data into a Pulsar topic.
 *
 * <p>Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @tparam T Type of the messages to write into Kafka.
 */
abstract class FlinkPulsarSinkBase[T](
    val parameters: Properties,
    val topicKeyExtractor: TopicKeyExtractor[T])
  extends RichSinkFunction[T]
  with CheckpointedFunction
  with Logging {

  import org.apache.flink.pulsar.SourceSinkUtils._
  import org.apache.flink.pulsar.PulsarOptions._

  val caseInsensitiveParams = validateSinkOptions(parameters.asScala.toMap)

  val (clientConf, producerConf, topicConf, adminUrl) =
    prepareConfForProducer(parameters.asScala.toMap)

  var doFlushOnCheckpoint: Boolean = flushOnCheckpoint(caseInsensitiveParams)

  // fail on write failure or just log them and went on
  val doFailOnWrite: Boolean = failOnWrite(caseInsensitiveParams)

  CachedPulsarClient.setCacheSize(clientCacheSize(caseInsensitiveParams))

  // Number of unacknowledged records
  var pendingRecords: Long = 0L

  val pendingRecordsLock: SerializableObject = new SerializableObject

  val (forcedTopic, topicName) = topicConf match {
    case Some(topic) => (true, topic)
    case None => (false, "")
  }

  if (!forcedTopic && topicKeyExtractor == null) {
    throw new IllegalArgumentException(
      s"You should either set output topic through $TOPIC_SINGLE option," +
        "or provide a TopicKeyExtractor to generate topic from each record dynamically.")
  }

  if (topicKeyExtractor != null) {
    ClosureCleaner.clean(
      topicKeyExtractor, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true)
  }

  // for test purpose, admin would not be initialized in unit test
  @transient var adminUsed = false
  @transient protected lazy val admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()

  def pulsarSchema: Schema[_]

  // reuse producer through the executor
  @transient protected lazy val singleProducer =
  if (forcedTopic) {
    SchemaUtils.uploadPulsarSchema(admin, topicName, pulsarSchema.getSchemaInfo)
    adminUsed = true
    createProducer(clientConf, producerConf, topicName, pulsarSchema)
  } else null
  @transient protected lazy val topic2Producer: mutable.Map[String, Producer[_]] =
    mutable.Map.empty

  // used to synchronize with Pulsar callbacks
  @transient @volatile protected var failedWrite: Throwable = _

  @transient protected lazy val sendCallback: BiConsumer[MessageId, Throwable] = {
    if (doFailOnWrite) {
      new BiConsumer[MessageId, Throwable]() {
        override def accept(t: MessageId, u: Throwable): Unit = {
          if (failedWrite == null && u == null) {
            acknowledgeMessage()
          } else if (failedWrite == null && u != null) {
            failedWrite = u
          } else { // failedWrite != null
            // do nothing and wait next checkForError to throw exception
          }
        }
      }
    } else {
      new BiConsumer[MessageId, Throwable]() {
        override def accept(t: MessageId, u: Throwable): Unit = {
          if (failedWrite == null && u != null) {
            logError(s"Error while sending message to Pulsar: $u")
          }
          acknowledgeMessage()
        }
      }
    }
  }

  override def open(parameters: Configuration): Unit = {
    if (doFlushOnCheckpoint &&
        !this.getRuntimeContext.asInstanceOf[StreamingRuntimeContext].isCheckpointingEnabled) {
      logWarning("Flushing on checkpoint is enabled, " +
        "but checkpointing is not enabled. Disabling flushing.")
      doFlushOnCheckpoint = false
    }
  }

  override def close(): Unit = {
    checkForErrors()
    producerClose()
    checkForErrors()
  }

  private def acknowledgeMessage(): Unit = {
    if (doFlushOnCheckpoint) {
      pendingRecordsLock.synchronized {
        pendingRecords -= 1
        if (pendingRecords == 0) {
          pendingRecordsLock.notifyAll()
        }
      }
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkForErrors()

    if (doFlushOnCheckpoint) {
      producerFlush()
      pendingRecordsLock.synchronized {
        if (pendingRecords != 0) {
          throw new IllegalStateException(
            s"Pending record count must be zero at this point: $pendingRecords");
        }
        checkForErrors()
      }
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  protected def checkForErrors(): Unit = {
    val e = failedWrite
    if (e != null) {
      failedWrite = null
      throw e
    }
  }

  protected def producerFlush(): Unit = {
    if (singleProducer != null) {
      singleProducer.flush()
    } else {
      topic2Producer.foreach(_._2.flush())
    }
    pendingRecordsLock.synchronized {
      while (pendingRecords > 0) {
        try {
          pendingRecordsLock.wait()
        } catch {
          case e: InterruptedException =>
            // this can be interrupted when the Task has been cancelled.
            // by throwing an exception, we ensure that this checkpoint doesn't get confirmed
            throw new RuntimeException("Flushing got interrupted while checkpointing", e)
        }
      }
    }
  }

  protected def producerClose(): Unit = {
    producerFlush()
    if (adminUsed) {
      admin.close()
    }
    if (singleProducer != null) {
      singleProducer.close()
    } else {
      topic2Producer.foreach(_._2.close())
    }
    topic2Producer.clear()
  }

  def getProducer[T](tp: String): Producer[T] = {
    if (null != singleProducer) {
      return singleProducer.asInstanceOf[Producer[T]]
    }

    if (topic2Producer.contains(tp)) {
      topic2Producer(tp).asInstanceOf[Producer[T]]
    } else {
      SchemaUtils.uploadPulsarSchema(admin, tp, pulsarSchema.getSchemaInfo)
      adminUsed = true
      val p =
        createProducer(clientConf, producerConf, tp, pulsarSchema)
      topic2Producer.put(tp, p)
      p.asInstanceOf[Producer[T]]
    }
  }

  private def createProducer[T](
    clientConf: ju.Map[String, Object],
    producerConf: ju.Map[String, Object],
    topic: String,
    schema: Schema[T]): Producer[T] = {

    CachedPulsarClient
      .getOrCreate(clientConf)
      .newProducer(schema)
      .topic(topic)
      .loadConf(producerConf)
      .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
      // maximizing the throughput
      .batchingMaxMessages(5 * 1024 * 1024)
      .create()
  }
}

trait TopicKeyExtractor[T] extends Serializable {
  def serializeKey(element: T) : Array[Byte]
  def getTopic(element: T): String
}

object DummyTopicKeyExtractor extends TopicKeyExtractor[Row] {
  override def serializeKey(element: Row): Array[Byte] = null
  override def getTopic(element: Row): String = null
}
