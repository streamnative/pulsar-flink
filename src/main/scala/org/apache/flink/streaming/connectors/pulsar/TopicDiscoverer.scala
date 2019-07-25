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

import java.util.regex.Pattern
import java.{util => ju}

import scala.collection.JavaConverters._
import org.apache.flink.connectors.pulsar.common.PulsarOptions.TOPIC_OPTION_KEYS
import org.apache.flink.connectors.pulsar.common.{Logging, PulsarOptions, SourceSinkUtils}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.naming.TopicName

class TopicDiscoverer(
  adminUrl: String,
  clientConf: ju.Map[String, Object],
  caseInsensitiveParameters: Map[String, String],
  indexOfThisSubtask: Int,
  numParallelSubtasks: Int)
  extends AutoCloseable with Logging {

  @volatile private var closed: Boolean = false

  private val admin: PulsarAdmin =
    PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()

  private var seenTopics: Set[String] = getTopicPartitions()

  def discoverTopicsChange(): (Set[String], Set[String]) = {
    if (!closed) {
      val currentTopics = getTopicPartitions()
      val removedTopics = seenTopics.diff(currentTopics)
      val addedTopics = currentTopics.diff(seenTopics)
      seenTopics = currentTopics
      (addedTopics, removedTopics)
    } else {
      throw new ClosedException
    }
  }

  def getTopicPartitions(): Set[String] = {
    val topics = getTopics()
    val topicPartitions = topics.flatMap { tp =>
      val partNum = admin.topics().getPartitionedTopicMetadata(tp).partitions
      if (partNum == 0) {
        tp :: Nil
      } else {
        (0 until partNum).map(tp + PulsarOptions.PARTITION_SUFFIX + _)
      }
    }
    topicPartitions
      .filter(SourceSinkUtils.belongsTo(_, numParallelSubtasks, indexOfThisSubtask))
      .toSet
  }

  private def getTopics(): Seq[String] = {
    val topics: Seq[String] = caseInsensitiveParameters.find(x => TOPIC_OPTION_KEYS.contains(x._1)).get match {
      case ("topic", value) =>
        TopicName.get(value).toString :: Nil
      case ("topics", value) =>
        value.split(",").map(_.trim).filter(_.nonEmpty).map(TopicName.get(_).toString)
      case ("topicspattern", value) =>
        getTopics(value)
    }
    topics
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

  override def close(): Unit = {
    closed = true
    admin.close()
  }
}

class ClosedException extends Exception
