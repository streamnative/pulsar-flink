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
package org.apache.flink.connectors.pulsar.common

import java.{util => ju}
import java.util.Locale

import org.apache.flink.connectors.pulsar.common.PulsarOptions._
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl, TopicMessageIdImpl}
import org.apache.pulsar.common.naming.TopicName

object SourceSinkUtils extends Logging {

  /**
    * Returns the index of the target subtask that a specific Pulsar partition should be
    * assigned to.
    */
  def belongsTo(topic: String, numParallelSubtasks: Int, index: Int): Boolean = {
    ((topic.hashCode * 31) & 0x7FFFFFFF) % numParallelSubtasks == index
  }

  def validateSourceOptions(
      caseInsensitiveParams: Map[String, String]): Map[String, String] = {
    if (!caseInsensitiveParams.contains(SERVICE_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(
        s"$SERVICE_URL_OPTION_KEY must be specified")
    }

    if (!caseInsensitiveParams.contains(ADMIN_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(
        s"$ADMIN_URL_OPTION_KEY must be specified")
    }

    // validate topic options
    val topicOptions = caseInsensitiveParams.filter {
      case (k, _) => TOPIC_OPTION_KEYS.contains(k)
    }.toSeq

    if (topicOptions.isEmpty || topicOptions.size > 1) {
      throw new IllegalArgumentException(
        "You should specify topic(s) using one of the topic options: "
          + TOPIC_OPTION_KEYS.mkString(", "))
    }

    caseInsensitiveParams
      .find(x => TOPIC_OPTION_KEYS.contains(x._1))
      .get match {
      case ("topic", value) =>
        if (value.contains(",")) {
          throw new IllegalArgumentException(
            """Use "topics" instead of "topic" for multi topic read""")
        } else if (value.trim.isEmpty) {
          throw new IllegalArgumentException("No topic is specified")
        }

      case ("topics", value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            s"No topics is specified for read with option: $value")
        }

      case ("topicspattern", value) =>
        if (value.trim.length == 0) {
          throw new IllegalArgumentException("TopicsPattern is empty")
        }
    }
    caseInsensitiveParams
  }

  def validateStreamSourceOptions(
      parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map {
      case (k, v) => (k.toLowerCase(Locale.ROOT), v)
    }
    caseInsensitiveParams
      .get(ENDING_OFFSETS_OPTION_KEY)
      .map(
        _ =>
          throw new IllegalArgumentException(
            "ending offset not valid in streaming queries"))

    validateSourceOptions(caseInsensitiveParams)
  }

  def validateSinkOptions(
      parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map {
      case (k, v) => (k.toLowerCase(Locale.ROOT), v)
    }

    if (!caseInsensitiveParams.contains(SERVICE_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(
        s"$SERVICE_URL_OPTION_KEY must be specified")
    }

    if (!caseInsensitiveParams.contains(ADMIN_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(
        s"$ADMIN_URL_OPTION_KEY must be specified")
    }

    val topicOptions =
      caseInsensitiveParams
        .filter { case (k, _) => TOPIC_OPTION_KEYS.contains(k) }
        .toSeq
        .toMap
    if (topicOptions.size > 1 || topicOptions.contains(TOPIC_MULTI) || topicOptions
          .contains(TOPIC_PATTERN)) {
      throw new IllegalArgumentException(
        "Currently, we only support specify single topic through option, " +
          s"use '$TOPIC_SINGLE' to specify it.")
    }

    caseInsensitiveParams
  }

  def getPulsarOffset(params: Map[String, String],
                      offsetOptionKey: String,
                      defaultOffsets: PulsarOffset): PulsarOffset = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
        LatestOffset
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
        EarliestOffset
      case Some(json) =>
        SpecificPulsarOffset(JsonUtils.topicOffsets(json))
      case None => defaultOffsets
    }
  }

  def paramsToPulsarConf(
      module: String,
      params: Map[String, String]): ju.Map[String, Object] = {
    PulsarConfigUpdater(module, params).rebuild()
  }

  private def getClientParams(
      parameters: Map[String, String]): Map[String, String] = {
    parameters.keySet
      .filter(_.startsWith(PULSAR_CLIENT_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_CLIENT_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
  }

  private def getReaderParams(parameters: Map[String, String]): Map[String, String] = {
    parameters.keySet
      .filter(_.startsWith(PULSAR_READER_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_READER_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
  }

  private def getProducerParams(
      parameters: Map[String, String]): Map[String, String] = {
    parameters.keySet
      .filter(_.startsWith(PULSAR_PRODUCER_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_PRODUCER_OPTION_KEY_PREFIX.length).toString -> parameters(
          k)
      }
      .toMap
  }

  def getServiceUrl(parameters: Map[String, String]): String = {
    parameters.get(SERVICE_URL_OPTION_KEY).get
  }

  def getAdminUrl(parameters: Map[String, String]): String = {
    parameters.get(ADMIN_URL_OPTION_KEY).get
  }

  def getPartitionDiscoveryIntervalInMillis(parameters: Map[String, String]): Long = {
    parameters.getOrElse(PARTITION_DISCOVERY_INTERVAL_MS, "-1L").toLong
  }

  def enableMetrics(parameters: Map[String, String]): Boolean = {
    parameters.getOrElse(USE_METRICS_OPTION_KEY, "true").toBoolean
  }

  def prepareConfForReader(parameters: Map[String, String])
    : (ju.Map[String, Object], ju.Map[String, Object], String, String) = {

    val serviceUrl = getServiceUrl(parameters)
    val adminUrl = getAdminUrl(parameters)

    var clientParams = getClientParams(parameters)
    clientParams += (SERVICE_URL_OPTION_KEY -> serviceUrl)
    val readerParams = getReaderParams(parameters)

    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.reader", readerParams),
      serviceUrl,
      adminUrl
    )
  }

  def prepareConfForProducer(parameters: Map[String, String]):
    (ju.Map[String, Object], ju.Map[String, Object], Option[String], String) = {

    val serviceUrl = getServiceUrl(parameters)
    val adminUrl = getAdminUrl(parameters)

    var clientParams = getClientParams(parameters)
    clientParams += (SERVICE_URL_OPTION_KEY -> serviceUrl)
    val producerParams = getProducerParams(parameters)

    val topic =
      parameters.get(TOPIC_SINGLE).map(_.trim).map(TopicName.get(_).toString)

    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.producer", producerParams),
      topic,
      adminUrl
    )
  }

  def failOnDataLoss(caseInsensitiveParams: Map[String, String]): Boolean =
    caseInsensitiveParams.getOrElse(FAIL_ON_DATA_LOSS_OPTION_KEY, "false").toBoolean

  def pollTimeoutMs(caseInsensitiveParams: Map[String, String]): Int =
    caseInsensitiveParams.getOrElse(PulsarOptions.POLL_TIMEOUT_MS, "120000").toInt


  def messageExists(mid: MessageId): Boolean = {
    mid match {
      case m: MessageIdImpl => m.getLedgerId != -1 && m.getEntryId != -1
      case t: TopicMessageIdImpl => messageExists(t.getInnerMessageId)
    }
  }

  def enteredEnd(end: MessageId)(current: MessageId): Boolean = {
    val endImpl = end.asInstanceOf[MessageIdImpl]
    val currentImpl = current.asInstanceOf[MessageIdImpl]
    val result = endImpl.getLedgerId == currentImpl.getLedgerId &&
      endImpl.getEntryId == currentImpl.getEntryId

    result
  }

  def isLastMessage(messageId: MessageId): Boolean = {
    messageId match {
      case bmid: BatchMessageIdImpl =>
        bmid.getBatchIndex == bmid.getBatchSize - 1
      case _: MessageIdImpl =>
        true
      case _ =>
        throw new IllegalStateException(
          s"reading a message of type ${messageId.getClass.getName}")
    }
  }

  def mid2Impl(mid: MessageId): MessageIdImpl = {
    mid match {
      case bmid: BatchMessageIdImpl =>
        new MessageIdImpl(bmid.getLedgerId, bmid.getEntryId, bmid.getPartitionIndex)
      case midi: MessageIdImpl => midi
      case t: TopicMessageIdImpl => mid2Impl(t.getInnerMessageId)
    }
  }

  def seekableLatestMid(mid: MessageId): MessageId = {
    if (messageExists(mid)) mid else MessageId.earliest
  }

  /**
    * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`.
    * Otherwise, just log a warning.
    */
  def reportDataLossFunc(failOnDataLoss: Boolean): (String) => Unit = { (message: String) =>
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE")
    } else {
      logWarning(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE")
    }
  }

  // used to check whether starting position and current message we got actually are equal
  // we neglect the potential batchIdx deliberately while seeking to MessageIdImpl for batch entry
  def messageIdRoughEquals(l: MessageId, r: MessageId): Boolean = {
    (l, r) match {
      case (lb: BatchMessageIdImpl, rb: BatchMessageIdImpl) => lb.equals(rb)
      case (lm: MessageIdImpl, rb: BatchMessageIdImpl) =>
        lm.equals(new MessageIdImpl(rb.getLedgerId, rb.getEntryId, rb.getPartitionIndex))
      case (lb: BatchMessageIdImpl, rm: MessageIdImpl) =>
        rm.equals(new MessageIdImpl(lb.getLedgerId, lb.getEntryId, lb.getPartitionIndex))
      case (lm: MessageIdImpl, rm: MessageIdImpl) => lm.equals(rm)
      case _ =>
        throw new IllegalStateException(
          s"comparing messageIds of type [${l.getClass.getName}, ${r.getClass.getName}]")
    }
  }

  def offsetForEachTopic(
    topics: Set[String],
    params: Map[String, String],
    offsetOptionKey: String,
    defaultOffsets: PulsarOffset): SpecificPulsarOffset = {

    val offset = getPulsarOffset(params, offsetOptionKey, defaultOffsets)
    offset match {
      case LatestOffset =>
        SpecificPulsarOffset(topics.map(tp => (tp, MessageId.latest)).toMap)
      case EarliestOffset =>
        SpecificPulsarOffset(topics.map(tp => (tp, MessageId.earliest)).toMap)
      case so: SpecificPulsarOffset =>
        val specified: Map[String, MessageId] = so.topicOffsets
        assert(
          specified.keySet.subsetOf(topics),
          s"topics designated in startingOffsets/endingOffsets" +
            s" should all appear in $TOPIC_OPTION_KEYS .\n" +
            s"topics: $topics, topics in offsets: ${specified.keySet}"
        )
        val nonSpecifiedTopics = topics -- specified.keySet
        val nonSpecified = nonSpecifiedTopics.map { tp =>
          defaultOffsets match {
            case LatestOffset => (tp, MessageId.latest)
            case EarliestOffset => (tp, MessageId.earliest)
            case _ => throw new IllegalArgumentException("Defaults should be latest or earliest")
          }
        }.toMap
        SpecificPulsarOffset(specified ++ nonSpecified)
    }
  }
}
