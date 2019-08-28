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
package org.apache.flink.table.catalog.pulsar.descriptors

import org.apache.flink.pulsar.PulsarOptions
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.{CatalogDescriptorValidator, DescriptorProperties}

class PulsarCatalogValidator extends CatalogDescriptorValidator {
  import PulsarCatalogValidator._
  import CatalogDescriptorValidator._

  override def validate(properties: DescriptorProperties): Unit = {
    super.validate(properties)
    properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR, false)
    properties.validateString(CATALOG_PULSAR_VERSION, true, 1)
    properties.validateString(CATALOG_SERVICE_URL, false, "pulsar://0.0.0.0:0".length)
    properties.validateString(CATALOG_ADMIN_URL, false, "http://0.0.0.0:0".length)
    validateStartingOffsets(properties)
  }


  def validateStartingOffsets(properties: DescriptorProperties): Unit = {
    if (properties.containsKey(CATALOG_STARTING_POS)) {
      val v = properties.getString(CATALOG_STARTING_POS)
      if (v != "earliest" && v != "latest") {
        throw new ValidationException(s"$CATALOG_STARTING_POS should be either earliest or latest")
      }
    }
  }
}

object PulsarCatalogValidator {
  val CATALOG_TYPE_VALUE_PULSAR = "pulsar"
  val CATALOG_PULSAR_VERSION = "pulsar-version"
  val CATALOG_SERVICE_URL = PulsarOptions.SERVICE_URL_OPTION_KEY
  val CATALOG_ADMIN_URL = PulsarOptions.ADMIN_URL_OPTION_KEY
  val CATALOG_STARTING_POS = PulsarOptions.STARTING_OFFSETS_OPTION_KEY
}
