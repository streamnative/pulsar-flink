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
package org.apache.flink.table.descriptors

import java.{util => ju}
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.flink.streaming.connectors.pulsar.TopicKeyExtractor
import org.apache.flink.table.descriptors.PulsarValidator._
import org.apache.flink.util.Preconditions

class PulsarC extends ConnectorDescriptor(CONNECTOR_TYPE_VALUE_PULSAR, 1, false) {

  var pulsarProperties: ju.Map[String, String] = _
  var sinkExtractorType: String = _
  var sinkExtractorClass: Class[_ <: TopicKeyExtractor[_]] = _

  /**
   * Sets the configuration properties for the Pulsar consumer. Resets previously set properties.
   *
   * @param properties The configuration properties for the Pulsar consumer.
   */
  def properties(properties: Properties): Pulsar = {
    Preconditions.checkNotNull(properties)
    if (this.pulsarProperties == null) {
      this.pulsarProperties = new ju.HashMap[String, String]
    }
    this.pulsarProperties.clear()
    val p = properties.asScala.foreach { case (k, v) =>
      pulsarProperties.put(k, v)
    }
    this
  }

  /**
   * Adds a configuration properties for the Pulsar consumer.
   *
   * @param key   property key for the Pulsar consumer
   * @param value property value for the Pulsar consumer
   */
  def property(key: String, value: String): Pulsar = {
    Preconditions.checkNotNull(key)
    Preconditions.checkNotNull(value)
    if (this.pulsarProperties == null) {
      this.pulsarProperties = new ju.HashMap[String, String]
    }
    pulsarProperties.put(key, value)
    this
  }

  def sinkExtractorNone(): Pulsar = {
    sinkExtractorType = CONNECTOR_SINK_EXTRACTOR_NONE
    sinkExtractorClass = null
    this
  }

  def sinkExtractorCustom(extractorClass: Class[_ <: TopicKeyExtractor[_]]): Pulsar = {
    sinkExtractorType = CONNECTOR_SINK_EXTRACTOR_CUSTOM
    sinkExtractorClass = Preconditions.checkNotNull(extractorClass)
    this
  }

  override def toConnectorProperties: ju.Map[String, String] = {
    val dp: DescriptorProperties = new DescriptorProperties()

    if (pulsarProperties != null) {
      val plist = pulsarProperties.asScala.map { case (k, v) =>
        ju.Arrays.asList(k, v)
      }.toList.asJava

      dp.putIndexedFixedProperties(
        CONNECTOR_PROPERTIES,
        ju.Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE),
        plist)
    }

    if (sinkExtractorType != null) {
      dp.putString(CONNECTOR_SINK_EXTRACTOR, sinkExtractorType)
      if (CONNECTOR_SINK_EXTRACTOR_CLASS != null) {
        dp.putClass(CONNECTOR_SINK_EXTRACTOR_CLASS, sinkExtractorClass)
      }
    }

    dp.asMap()
  }
}
