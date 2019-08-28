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

import java.util

import PulsarCatalogValidator._

import org.apache.flink.table.descriptors.{CatalogDescriptor, DescriptorProperties}
import org.apache.flink.util.{Preconditions, StringUtils}

class PulsarCatalogDescriptor extends
    CatalogDescriptor(CATALOG_TYPE_VALUE_PULSAR, 1, "public/default") {

  var pulsarVersion: String = null

  def pulsarVersion(pulsarVersion: String): PulsarCatalogDescriptor = {
    Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(pulsarVersion))
    this.pulsarVersion = pulsarVersion
    this
  }

  override def toCatalogProperties: util.Map[String, String] = {
    val props = new DescriptorProperties()

    if (pulsarVersion != null) {
      props.putString(CATALOG_PULSAR_VERSION, pulsarVersion)
    }

    props.asMap()
  }
}
