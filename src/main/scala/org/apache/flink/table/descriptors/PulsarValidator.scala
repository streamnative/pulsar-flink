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

import java.util
import java.util.function.Consumer

import scala.compat.java8.FunctionConverters._

import org.apache.flink.table.descriptors.DescriptorProperties.noValidation

class PulsarValidator extends ConnectorDescriptorValidator {

  import PulsarValidator._
  import ConnectorDescriptorValidator._

  override def validate(properties: DescriptorProperties): Unit = {
//    super.validate(properties)
//
//    properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PULSAR, false)

    validatePulsarProperties(properties)

    validateSinkExtractor(properties)
  }

  private def validatePulsarProperties(properties: DescriptorProperties): Unit = {
    val propertyValidators = new util.HashMap[String, Consumer[String]]

    propertyValidators.put(
      CONNECTOR_PROPERTIES_KEY,
      asJavaConsumer(k => properties.validateString(k, false, 1)))

    propertyValidators.put(
      CONNECTOR_PROPERTIES_VALUE,
      asJavaConsumer(k => properties.validateString(k, false, 0))
    )
    properties.validateFixedIndexedProperties(CONNECTOR_PROPERTIES, true, propertyValidators)
  }

  private def validateSinkExtractor(properties: DescriptorProperties): Unit = {
    val sinkValidators = new util.HashMap[String, Consumer[String]]

    sinkValidators.put(CONNECTOR_SINK_EXTRACTOR_NONE, noValidation())
    sinkValidators.put(
      CONNECTOR_SINK_EXTRACTOR_CUSTOM,
      asJavaConsumer(k => properties.validateString(CONNECTOR_SINK_EXTRACTOR_CLASS, false, 1))
    )
    properties.validateEnum(CONNECTOR_SINK_EXTRACTOR, true, sinkValidators)
  }

}

object PulsarValidator {
  val CONNECTOR_TYPE_VALUE_PULSAR = "pulsar"
  val CONNECTOR_PROPERTIES = "connector.properties"
  val CONNECTOR_PROPERTIES_KEY = "key"
  val CONNECTOR_PROPERTIES_VALUE = "value"
  val CONNECTOR_SINK_EXTRACTOR = "connector.sink-extractor"
  val CONNECTOR_SINK_EXTRACTOR_CLASS = "connector.sink-extractor-class"
  val CONNECTOR_SINK_EXTRACTOR_NONE = "none"
  val CONNECTOR_SINK_EXTRACTOR_CUSTOM = "custom"

}
