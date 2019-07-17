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
import java.io.Closeable
import java.util.{Optional, UUID}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.pulsar.client.admin.{PulsarAdmin, PulsarAdminException}
import org.apache.pulsar.client.api.{Message, MessageId, PulsarClient, SubscriptionType}
import org.apache.pulsar.client.impl.schema.BytesSchema
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaInfo

/**
 * A Helper class that responsible for:
 * - getEarliest / Latest / Specific MessageIds
 * - guarantee message existence using subscription by setup, move and remove
 */
private[pulsar] case class PulsarMetadataReader(
    serviceUrl: String,
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    driverGroupIdPrefix: String,
    caseInsensitiveParameters: Map[String, String])
    extends Closeable
    with Logging {

  import scala.collection.JavaConverters._

  protected val admin: PulsarAdmin =
    PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()
  protected var client: PulsarClient = null

  private var topics: Seq[String] = _
  private var topicPartitions: Seq[String] = _

  override def close(): Unit = {
    admin.close()
    if (client != null) {
      client.close()
    }
  }

  def setupCursor(offset: SpecificPulsarOffset): Unit = {
    offset.topicOffsets.foreach {
      case (tp, mid) =>
        try {
          admin.topics().createSubscription(tp, s"$driverGroupIdPrefix-$tp", mid)
        } catch {
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to create schema for ${TopicName.get(tp).toString}",
              e)
        }
    }
  }

  def commitCursorToOffset(offset: Map[String, MessageId]): Unit = {
    offset.foreach {
      case (tp, mid) =>
        try {
          admin.topics().resetCursor(tp, s"$driverGroupIdPrefix-$tp", mid)
        } catch {
          case e: PulsarAdminException if e.getStatusCode == 404 || e.getStatusCode == 412 =>
            logInfo(
              s"Cannot commit cursor since the topic $tp has been deleted during execution.")
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to commit cursor for ${TopicName.get(tp).toString}",
              e)
        }
    }
  }

  def removeCursor(): Unit = {
    getTopics()
    topics.foreach { tp =>
      try {
        admin.topics().deleteSubscription(tp, s"$driverGroupIdPrefix-$tp")
      } catch {
        case e: PulsarAdminException if e.getStatusCode == 404 =>
          logInfo(s"Cannot remove cursor since the topic $tp has been deleted during execution.")
        case e: Throwable =>
          throw new RuntimeException(
            s"Failed to remove cursor for ${TopicName.get(tp).toString}",
            e)
      }
    }
  }

  def getAndCheckCompatible(schema: Option[StructType]): StructType = {
    val inferredSchema = getSchema()
    require(
      schema.isEmpty || inferredSchema == schema.get,
      "The Schema of Pulsar source and provided doesn't match")
    inferredSchema
  }

  def getAndCheckCompatible(schema: Optional[StructType]): StructType = {
    val inferredSchema = getSchema()
    require(
      !schema.isPresent || inferredSchema == schema.get,
      "The Schema of Pulsar source and provided doesn't match")
    inferredSchema
  }

  def getSchema(): StructType = {
    val si = getPulsarSchema()
    SchemaUtils.pulsarSourceSchema(si)
  }

  def getPulsarSchema(): SchemaInfo = {
    getTopics()
    if (topics.size > 0) {
      val schemas = topics.map { tp =>
        getPulsarSchema(tp)
      }
      val sset = schemas.toSet
      if (sset.size != 1) {
        throw new IllegalArgumentException(
          s"Topics to read must share identical schema, " +
            s"however we got ${sset.size} distinct schemas:[${sset.mkString(", ")}]")
      }
      sset.head
    } else {
      // if no topic exists, and we are getting schema, then auto created topic has schema of None
      SchemaUtils.emptySchemaInfo()
    }
  }

  def getPulsarSchema(topic: String): SchemaInfo = {
    try {
      admin.schemas().getSchemaInfo(TopicName.get(topic).toString)
    } catch {
      case e: PulsarAdminException if e.getStatusCode == 404 =>
        return BytesSchema.of().getSchemaInfo
      case e: Throwable =>
        throw new RuntimeException(
          s"Failed to get schema information for ${TopicName.get(topic).toString}",
          e)
    }
  }

  def fetchLatestOffsets(): SpecificPulsarOffset = {
    getTopicPartitions()
    SpecificPulsarOffset(topicPartitions.map { tp =>
      (tp -> PulsarSourceUtils.seekableLatestMid(
        try {
          admin.topics().getLastMessageId(tp)
        } catch {
          case e: PulsarAdminException if e.getStatusCode == 404 =>
            MessageId.earliest
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to get last messageId for ${TopicName.get(tp).toString}",
              e)
        }
      ))
    }.toMap)
  }

  def fetchLatestOffsetForTopic(topic: String): MessageId = {
    PulsarSourceUtils.seekableLatestMid( try {
      admin.topics().getLastMessageId(topic)
    } catch {
      case e: PulsarAdminException if e.getStatusCode == 404 =>
        MessageId.earliest
      case e: Throwable =>
        throw new RuntimeException(
          s"Failed to get last messageId for ${TopicName.get(topic).toString}",
          e)
    })
  }

  def fetchEarliestOffsets(topics: Seq[String]): Map[String, MessageId] = {
    if (topics.isEmpty) {
      Map.empty[String, MessageId]
    } else {
      topics.map(p => p -> MessageId.earliest).toMap
    }
  }

  private def getTopics(): Seq[String] = {
    topics = caseInsensitiveParameters.find(x => TOPIC_OPTION_KEYS.contains(x._1)).get match {
      case ("topic", value) =>
        TopicName.get(value).toString :: Nil
      case ("topics", value) =>
        value.split(",").map(_.trim).filter(_.nonEmpty).map(TopicName.get(_).toString)
      case ("topicspattern", value) =>
        getTopics(value)
    }
    topics
  }

  private def getTopicPartitions(): Seq[String] = {
    getTopics()
    topicPartitions = topics.flatMap { tp =>
      val partNum = admin.topics().getPartitionedTopicMetadata(tp).partitions
      if (partNum == 0) {
        tp :: Nil
      } else {
        (0 until partNum).map(tp + PulsarOptions.PARTITION_SUFFIX + _)
      }
    }
    topicPartitions
  }

  private def getTopics(topicsPattern: String): Seq[String] = {
    val dest = TopicName.get(topicsPattern)
    val allNonPartitionedTopics: ju.List[String] =
      admin
        .topics()
        .getList(dest.getNamespace)
        .asScala
        .filter(t => !TopicName.get(t).isPartitioned)
        .asJava
    val nonPartitionedMatch = topicsPatternFilter(allNonPartitionedTopics, dest.toString)

    val allPartitionedTopics: ju.List[String] =
      admin.topics().getPartitionedTopicList(dest.getNamespace)
    val partitionedMatch = topicsPatternFilter(allPartitionedTopics, dest.toString)
    nonPartitionedMatch ++ partitionedMatch
  }

  private def topicsPatternFilter(
      allTopics: ju.List[String],
      topicsPattern: String): Seq[String] = {
    val shortenedTopicsPattern = Pattern.compile(topicsPattern.split("\\:\\/\\/")(1))
    allTopics.asScala
      .map(TopicName.get(_).toString)
      .filter(tp => shortenedTopicsPattern.matcher(tp.split("\\:\\/\\/")(1)).matches())
  }

  def offsetForEachTopic(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: PulsarOffset): SpecificPulsarOffset = {

    getTopicPartitions()
    val offset = PulsarProvider.getPulsarOffset(params, offsetOptionKey, defaultOffsets)
    offset match {
      case LatestOffset =>
        SpecificPulsarOffset(topicPartitions.map(tp => (tp, MessageId.latest)).toMap)
      case EarliestOffset =>
        SpecificPulsarOffset(topicPartitions.map(tp => (tp, MessageId.earliest)).toMap)
      case so: SpecificPulsarOffset =>
        val specified: Map[String, MessageId] = so.topicOffsets
        assert(
          specified.keySet.subsetOf(topicPartitions.toSet),
          s"topics designated in startingOffsets/endingOffsets" +
            s" should all appear in $TOPIC_OPTION_KEYS .\n" +
            s"topics: $topicPartitions, topics in offsets: ${specified.keySet}"
        )
        val nonSpecifiedTopics = topicPartitions.toSet -- specified.keySet
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

  def fetchCurrentOffsets(
      offset: SpecificPulsarOffset,
      poolTimeoutMs: Option[Int],
      reportDataLoss: String => Unit): Map[String, MessageId] = {

    offset.topicOffsets.map {
      case (tp, off) =>
        val actualOffset = off match {
          case MessageId.earliest =>
            off
          case MessageId.latest =>
            PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp))
          case _ =>
            if (client == null) {
              client = PulsarClient.builder().serviceUrl(serviceUrl).build()
            }
            val consumer = client
              .newConsumer()
              .topic(tp)
              .subscriptionName(s"spark-pulsar-${UUID.randomUUID()}")
              .subscriptionType(SubscriptionType.Exclusive)
              .subscribe()
            consumer.seek(off)
            var msg: Message[Array[Byte]] = null
            if (poolTimeoutMs.isDefined) {
              msg = consumer.receive(poolTimeoutMs.get, TimeUnit.MILLISECONDS)
            } else {
              msg = consumer.receive()
            }
            consumer.close()
            if (msg == null) {
              MessageId.earliest
            } else {
              PulsarSourceUtils.mid2Impl(msg.getMessageId)
            }
        }
        (tp, actualOffset)
    }
  }
}
