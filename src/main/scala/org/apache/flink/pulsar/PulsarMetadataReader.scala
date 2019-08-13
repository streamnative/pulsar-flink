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
package org.apache.flink.pulsar

import java.{util => ju}
import java.io.Closeable
import java.util.Optional
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.flink.table.types.{DataType, FieldsDataType}

import org.apache.pulsar.client.admin.{PulsarAdmin, PulsarAdminException}
import org.apache.pulsar.client.api.{MessageId, PulsarClient}
import org.apache.pulsar.client.impl.schema.BytesSchema
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaInfo

/**
 * A Helper class that responsible for:
 * - getEarliest / Latest / Specific MessageIds
 * - guarantee message existence using subscription by setup, move and remove
 */
case class PulsarMetadataReader(
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    driverGroupIdPrefix: String = "",
    caseInsensitiveParams: Map[String, String],
    indexOfThisSubtask: Int = -1,
    numParallelSubtasks: Int = -1)
    extends Closeable
    with Logging {

  protected lazy val admin: PulsarAdmin =
    PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()
  protected var client: PulsarClient = null

  @volatile private var closed: Boolean = false

  private var seenTopics: Set[String] = Set.empty

  def discoverTopicsChange(): Set[String] = {
    if (!closed) {
      val currentTopics = getTopicPartitions()
      val addedTopics = currentTopics.diff(seenTopics)
      seenTopics = currentTopics
      addedTopics
    } else {
      throw new ClosedException
    }
  }

  override def close(): Unit = {
    closed = true
    admin.close()
    if (client != null) {
      client.close()
    }
  }

  def setupCursor(offset: Map[String, MessageId]): Unit = {
    offset.foreach {
      case (tp, mid) =>
        try {
          admin.topics().createSubscription(tp, driverGroupIdPrefix, mid)
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
          admin.topics().resetCursor(tp, driverGroupIdPrefix, mid)
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

  def removeCursor(topics: Seq[String]): Unit = {
    topics.foreach { tp =>
      try {
        admin.topics().deleteSubscriptionAsync(tp, driverGroupIdPrefix)
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

  def getAndCheckCompatible(topics: Seq[String], schema: Option[DataType]): FieldsDataType = {
    val inferredSchema = getSchema(topics)
    require(
      schema.isEmpty || inferredSchema == schema.get,
      "The Schema of Pulsar source and provided doesn't match")
    inferredSchema
  }

  def getAndCheckCompatible(topics: Seq[String], schema: Optional[DataType]): FieldsDataType = {
    val inferredSchema = getSchema(topics)
    require(
      !schema.isPresent || inferredSchema == schema.get,
      "The Schema of Pulsar source and provided doesn't match")
    inferredSchema
  }

  def getSchema(topics: Seq[String]): FieldsDataType = {
    val si = getPulsarSchema(topics)
    SchemaUtils.pulsarSourceSchema(si)
  }

  def getPulsarSchema(topics: Seq[String]): SchemaInfo = {
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

  def getTopicPartitions(): Set[String] = {
    val tps = getTopicPartitionsAll()
    tps.filter(SourceSinkUtils.belongsTo(_, numParallelSubtasks, indexOfThisSubtask))
  }

  def getTopicPartitionsAll(): Set[String] = {
    val topics = getTopics()
    val topicPartitions = topics.flatMap { tp =>
      val partNum = admin.topics().getPartitionedTopicMetadata(tp).partitions
      if (partNum == 0) {
        tp :: Nil
      } else {
        (0 until partNum).map(tp + PulsarOptions.PARTITION_SUFFIX + _)
      }
    }
    topicPartitions.toSet
  }

  def getTopics(): Seq[String] = {
    caseInsensitiveParams.find(x => PulsarOptions.TOPIC_OPTION_KEYS.contains(x._1)).get match {
      case ("topic", value) =>
        TopicName.get(value).toString :: Nil
      case ("topics", value) =>
        value.split(",").map(_.trim).filter(_.nonEmpty).map(TopicName.get(_).toString)
      case ("topicspattern", value) =>
        getTopics(value)
    }
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
}

class ClosedException extends Exception
