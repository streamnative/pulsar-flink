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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.{DescriptorProperties, RowtimeValidator, SchemaValidator}
import org.apache.flink.table.descriptors.Rowtime.ROWTIME
import org.apache.flink.table.descriptors.Schema.{SCHEMA, SCHEMA_FROM, SCHEMA_NAME, SCHEMA_PROCTIME, SCHEMA_TYPE}

case class PulsarSchemaValidator(
    isStreamEnvironment: Boolean,
    supportsSourceTimestamps: Boolean,
    supportsSourceWatermarks: Boolean) extends SchemaValidator(
    isStreamEnvironment, supportsSourceTimestamps, supportsSourceWatermarks) {

  // derived from SchemaValidator
  // remove the need for schema when proctime or rowtime is not defined
  override def validate(properties: DescriptorProperties): Unit = {
    val names: ju.Map[String, String] = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME)
    val types: ju.Map[String, String] = properties.getIndexedProperty(SCHEMA, SCHEMA_TYPE)

    var proctimeFound: Boolean = false

    val fieldsCount = Math.max(names.size, types.size)

    var i: Int = 0
    while (i < fieldsCount) {
      properties.validateString(SCHEMA + "." + i + "." + SCHEMA_NAME, false, 1)
      properties.validateType(SCHEMA + "." + i + "." + SCHEMA_TYPE, false, false)
      properties.validateString(SCHEMA + "." + i + "." + SCHEMA_FROM, true, 1)
      // either proctime or rowtime
      val proctime = SCHEMA + "." + i + "." + SCHEMA_PROCTIME
      val rowtime = SCHEMA + "." + i + "." + ROWTIME

      if (properties.containsKey(proctime)) {
        if (!isStreamEnvironment) {
          throw new ValidationException(
            s"Property $proctime is not allowed in a batch environment.")
        } else if (proctimeFound) {
          throw new ValidationException("A proctime attribute must only be defined once.")
        }
        // check proctime
        properties.validateBoolean(proctime, false)
        proctimeFound = properties.getBoolean(proctime)
        // no rowtime
        properties.validatePrefixExclusion(rowtime)
      } else if (properties.hasPrefix(rowtime)) {
        // check rowtime
        val rowtimeValidator =
          new RowtimeValidator(
            supportsSourceTimestamps, supportsSourceWatermarks, SCHEMA + "." + i + ".")
        rowtimeValidator.validate(properties)
        // no proctime
        properties.validateExclusion(proctime)
      }

      i += 1
    }
  }
}
