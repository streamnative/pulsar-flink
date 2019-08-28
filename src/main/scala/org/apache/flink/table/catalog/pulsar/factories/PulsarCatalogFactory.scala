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
package org.apache.flink.table.catalog.pulsar.factories

import java.{util => ju}

import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.catalog.pulsar.PulsarCatalog
import org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.factories.CatalogFactory

class PulsarCatalogFactory extends CatalogFactory {

  import org.apache.flink.table.descriptors.CatalogDescriptorValidator._
  import org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator._

  override def createCatalog(name: String, props: ju.Map[String, String]): Catalog = {
    val descriptorProperties = getValidatedProperties(props)

    val defaultDatabase = descriptorProperties.getOptionalString(CATALOG_DEFAULT_DATABASE)
      .orElse("public/default")
    val adminUrl = descriptorProperties.getString(CATALOG_ADMIN_URL)

    new PulsarCatalog(adminUrl, name, descriptorProperties.asMap, defaultDatabase)
  }

  override def requiredContext(): ju.Map[String, String] = {
    val context = new ju.HashMap[String, String]()
    context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR)
    context.put(CATALOG_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties(): ju.List[String] = {
    val props = new ju.ArrayList[String]()
    props.add(CATALOG_DEFAULT_DATABASE)
    props.add(CATALOG_PULSAR_VERSION)
    props
  }

  def getValidatedProperties(properties: ju.Map[String, String]): DescriptorProperties = {
    val descriptorProperties = new DescriptorProperties(true)
    descriptorProperties.putProperties(properties)
    new PulsarCatalogValidator().validate(descriptorProperties)
    descriptorProperties
  }
}
