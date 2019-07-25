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
import java.util.concurrent.TimeUnit

import org.apache.flink.connectors.pulsar.common.PulsarOptions.INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE
import org.apache.flink.connectors.pulsar.common.{CachedPulsarClient, Logging, PulsarDeserializer, PulsarTopicState}
import org.apache.pulsar.client.api.{Message, MessageId, Reader}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}

class ReaderThread(
    owner: PulsarFetcher,
    state: PulsarTopicState,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    pollTimeoutMs: Int,
    exceptionProxy: ExceptionProxy)
    extends Thread with Logging {

  import org.apache.flink.connectors.pulsar.common.SourceSinkUtils._

  val topic = state.topic
  val startingOffsets = state.offset

  @volatile private var running: Boolean = true

  @volatile private var reader: Reader[Array[Byte]] = null

  private val deserilizer = new PulsarDeserializer()

  override def run(): Unit = {
    logInfo(s"Starting to fetch from $topic at $startingOffsets")

    try {

      reader = CachedPulsarClient
        .getOrCreate(clientConf)
        .newReader()
        .topic(topic)
        .startMessageId(startingOffsets)
        .startMessageIdInclusive()
        .loadConf(readerConf)
        .create()

      var currentMessage: Message[_] = null
      var currentId: MessageId = null

      // skip the first message since it has been processed already
      if (startingOffsets != MessageId.earliest) {
        currentMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
        if (currentMessage == null) {
          reportDataLoss(s"Cannot read data at offset $startingOffsets from topic: $topic")
        } else {
          currentId = currentMessage.getMessageId
          if (!messageIdRoughEquals(currentId, startingOffsets)) {
            reportDataLoss(
              s"Potential Data Loss in reading $topic: intended to start at $startingOffsets, " +
                s"actually we get $currentId")
          }

          (startingOffsets, currentId) match {
            case (_: BatchMessageIdImpl, _: BatchMessageIdImpl) =>
            // we seek using a batch message id, we can read next directly later
            case (_: MessageIdImpl, cbmid: BatchMessageIdImpl) =>
              // we seek using a message id, this is supposed to be read by previous task since it's
              // inclusive for the checkpoint, so we skip this batch
              val newStart =
                new MessageIdImpl(cbmid.getLedgerId, cbmid.getEntryId + 1, cbmid.getPartitionIndex)
              reader.seek(newStart)
            case (smid: MessageIdImpl, cmid: MessageIdImpl) =>
            // current entry is a non-batch entry, we can read next directly later
          }
        }
      } else {
        currentId = MessageId.earliest
      }

      logInfo(s"Starting to read $topic with reader thread $getName")

      while (running) {
        val message = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
        if (message != null) {
          val messageId = message.getMessageId
          val record = deserilizer.deserialize(message)
          owner.emitRecord(record, state, messageId)
        }
      }

    } catch {
      case t: Throwable =>
        exceptionProxy.reportError(t)
    } finally {
      if (reader != null) {
        try {
          reader.close()
        } catch {
          case t: Throwable =>
            logError(s"Error while closing Pulsar reader $t")
        }
      }
    }
  }

  def cancel(): Unit = {
    this.running = false

    if (reader != null) {
      reader.close()
    }

    this.interrupt()
  }

  def isRunning: Boolean = running

  def reportDataLoss(message: String): Unit = {
    running = false
    exceptionProxy.reportError(
      new IllegalStateException(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE"))
  }
}
