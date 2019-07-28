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

import org.apache.pulsar.client.api.MessageId

sealed trait PulsarOffset

case object EarliestOffset extends PulsarOffset

case object LatestOffset extends PulsarOffset

case class SpecificPulsarOffset(topicOffsets: Map[String, MessageId])
    extends PulsarOffset
