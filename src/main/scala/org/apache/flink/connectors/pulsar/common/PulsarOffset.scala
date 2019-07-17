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

import org.apache.pulsar.client.api.MessageId

private[pulsar] sealed trait PulsarOffset

private[pulsar] case object EarliestOffset extends PulsarOffset

private[pulsar] case object LatestOffset extends PulsarOffset

private[pulsar] case class SpecificPulsarOffset(topicOffsets: Map[String, MessageId])
    extends PulsarOffset {

  val json = JsonUtils.topicOffsets(topicOffsets)
}

private[pulsar] case class PulsarPartitionOffset(topic: String, messageId: MessageId)

private[pulsar] object SpecificPulsarOffset {

  def getTopicOffsets(offset: Offset): Map[String, MessageId] = {
    offset match {
      case o: SpecificPulsarOffset => o.topicOffsets
      case so: SerializedOffset => SpecificPulsarOffset(so).topicOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to PulsarSourceOffset")
    }
  }

  def apply(offset: SerializedOffset): SpecificPulsarOffset =
    SpecificPulsarOffset(JsonUtils.topicOffsets(offset.json))

  def apply(offsetTuples: (String, MessageId)*): SpecificPulsarOffset = {
    SpecificPulsarOffset(offsetTuples.toMap)
  }
}
