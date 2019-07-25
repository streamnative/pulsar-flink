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
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    driverGroupIdPrefix: String)
    extends Closeable
    with Logging {

  import scala.collection.JavaConverters._
  import PulsarOptions._

  protected val admin: PulsarAdmin =
    PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()
  protected var client: PulsarClient = null

  override def close(): Unit = {
    admin.close()
    if (client != null) {
      client.close()
    }
  }

  def setupCursor(offset: Map[String, MessageId]): Unit = {
    offset.foreach {
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

  def removeCursor(topics: Seq[String]): Unit = {
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
}
