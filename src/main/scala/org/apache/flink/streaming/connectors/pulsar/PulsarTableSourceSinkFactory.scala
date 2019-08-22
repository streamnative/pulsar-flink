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
import java.util.{Optional, Properties}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.java8.OptionConverters._

import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.descriptors.{DescriptorProperties, PulsarValidator, SchemaValidator}
import org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT
import org.apache.flink.table.descriptors.Rowtime.{ROWTIME_TIMESTAMPS_CLASS, ROWTIME_TIMESTAMPS_FROM, ROWTIME_TIMESTAMPS_SERIALIZED, ROWTIME_TIMESTAMPS_TYPE, ROWTIME_WATERMARKS_CLASS, ROWTIME_WATERMARKS_DELAY, ROWTIME_WATERMARKS_SERIALIZED, ROWTIME_WATERMARKS_TYPE}
import org.apache.flink.table.descriptors.Schema.{SCHEMA, SCHEMA_FROM, SCHEMA_NAME, SCHEMA_PROCTIME, SCHEMA_TYPE}
import org.apache.flink.table.factories.{StreamTableSinkFactory, StreamTableSourceFactory}
import org.apache.flink.table.sinks.StreamTableSink
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row

class PulsarTableSourceSinkFactory
    extends StreamTableSourceFactory[Row]
    with StreamTableSinkFactory[Row] {

  import org.apache.flink.table.descriptors.PulsarValidator._
  import org.apache.flink.table.descriptors.StreamTableDescriptorValidator._
  import org.apache.flink.table.descriptors.ConnectorDescriptorValidator._

  override def createStreamTableSource(
    properties: ju.Map[String, String]): StreamTableSource[Row] = {

    val dp = getValidatedProperties(properties)

    PulsarTableSource(
      getPulsarProperties(dp),
      SchemaValidator.deriveProctimeAttribute(dp).asScala,
      SchemaValidator.deriveRowtimeAttributes(dp).asScala)
  }

  override def createStreamTableSink(
    properties: ju.Map[String, String]): StreamTableSink[Row] = {

    val dp = getValidatedProperties(properties)
    val schema = dp.getTableSchema(SCHEMA)

    val proctime = SchemaValidator.deriveProctimeAttribute(dp)
    val rowtimeAttributeDescriptors = SchemaValidator.deriveRowtimeAttributes(dp)

    // see also FLINK-9870
    if (proctime.isPresent ||
      !rowtimeAttributeDescriptors.isEmpty ||
      checkForCustomFieldMapping(dp, schema)) {
     throw new TableException("Time attributes and custom field mappings are not supported yet.")
    }

    PulsarTableSink(
      schema,
      getPulsarProperties(dp))
  }

  private def getPulsarProperties(descriptorProperties: DescriptorProperties) = {
    val pulsarProperties = new Properties
    val propsList = descriptorProperties.getFixedIndexedProperties(
      CONNECTOR_PROPERTIES,
      ju.Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE))

    propsList.asScala.foreach{ case kv =>
        pulsarProperties.put(
          descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
          descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_VALUE)))
    }
    pulsarProperties
  }

  private def getValidatedProperties(
    properties: ju.Map[String, String]): DescriptorProperties = {

    val descriptorProperties: DescriptorProperties = new DescriptorProperties(true)
    descriptorProperties.putProperties(properties)
    // TODO allow Pulsar timestamps to be used, watermarks can not be received from source
    PulsarSchemaValidator(true, true, false).validate(descriptorProperties)
    new PulsarValidator().validate(descriptorProperties)
    descriptorProperties
  }

  override def requiredContext(): ju.Map[String, String] = {
    val ctx = mutable.HashMap.empty[String, String]
    ctx.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND) // append mode
    ctx.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PULSAR) // pulsar
    ctx.put(CONNECTOR_PROPERTY_VERSION, "1") // backwards compatibility
    ctx.asJava
  }

  override def supportedProperties(): ju.List[String] = {
    val properties = mutable.ListBuffer.empty[String]

    // Pulsar
    properties += (CONNECTOR_PROPERTIES)
    properties += (CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY)
    properties += (CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE)
    properties += (CONNECTOR_SINK_EXTRACTOR)
    properties += (CONNECTOR_SINK_EXTRACTOR_CLASS)

    // schema
    properties += (SCHEMA + ".#." + SCHEMA_TYPE)
    properties += (SCHEMA + ".#." + SCHEMA_NAME)
    properties += (SCHEMA + ".#." + SCHEMA_FROM)

    // time attributes
    properties += (SCHEMA + ".#." + SCHEMA_PROCTIME)
    properties += (SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE)
    properties += (SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM)
    properties += (SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS)
    properties += (SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED)
    properties += (SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE)
    properties += (SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS)
    properties += (SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED)
    properties += (SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY)

    // format wildcard
    properties += (FORMAT + ".*")

    properties.asJava
  }

  private def checkForCustomFieldMapping(dp: DescriptorProperties, schema: TableSchema) = {
    // until FLINK-9870 is fixed we assume that the table schema is the output type
    val fieldMapping = SchemaValidator.deriveFieldMapping(dp, Optional.of(schema.toRowType))
    fieldMapping.size != schema.getFieldNames.length ||
      !fieldMapping.asScala.filterNot { case (k, v) => k == v }.isEmpty
  }
}
