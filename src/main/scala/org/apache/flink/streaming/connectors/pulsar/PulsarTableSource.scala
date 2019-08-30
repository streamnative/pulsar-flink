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
import java.util.Properties

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

import org.apache.flink.pulsar.{Logging, PulsarMetadataReader, SchemaUtils, Utils}
import org.apache.flink.pulsar.SourceSinkUtils.prepareConfForReader
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableSchema, Types, ValidationException}
import org.apache.flink.table.sources.{DefinedProctimeAttribute, DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

case class PulsarTableSource(
    properties: Properties,
    proctimeAttribute: Option[String],
    rowtimeAttributeDescriptors: Seq[RowtimeAttributeDescriptor],
    providedSchema: Option[TableSchema])
    extends StreamTableSource[Row]
    with DefinedProctimeAttribute
    with DefinedRowtimeAttributes
    with Logging {

  validateProctimeAttribute(proctimeAttribute)
  validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors)

  lazy val (clientConf, readerConf, serviceUrl, adminUrl) =
    prepareConfForReader(properties.asScala.toMap)

  lazy val inferredSchema = Utils.tryWithResource(
    PulsarMetadataReader(adminUrl, clientConf, "", properties.asScala.toMap)) { reader =>
    val topics = reader.getTopics()
    reader.getSchema(topics)
  }

  lazy val schema = SchemaUtils.toTableSchema(inferredSchema)

  override def getDataStream(env: StreamExecutionEnvironment): DataStream[Row] = {
    val source = new FlinkPulsarSource(properties)
    env.addSource(source).name(explainSource)
  }

  override def getProctimeAttribute: String = proctimeAttribute.getOrElse(null)

  override def getRowtimeAttributeDescriptors: ju.List[RowtimeAttributeDescriptor] =
    rowtimeAttributeDescriptors.asJava

  override def getTableSchema: TableSchema = if (providedSchema.isDefined) {
    providedSchema.get
  } else schema

  override def getProducedDataType: DataType = getTableSchema().toRowDataType

  /**
   * Validates a field of the schema to be the processing time attribute.
   *
   * @param proctimeAttribute The name of the field that becomes the processing time field.
   */
  private def validateProctimeAttribute(
      proctimeAttribute: Option[String]): Option[String] = {
    proctimeAttribute match {
      case Some(attr) =>
        val tpe = schema.getFieldType(attr).asScala
        tpe match {
          case Some(dt) if dt != Types.SQL_TIMESTAMP =>
            throw new ValidationException(
              s"Processing time attribute '$attr' is not of type SQL_TIMESTAMP.")
          case None =>
            throw new ValidationException(
              s"Processing time attribute '$attr' is not present in TableSchema.")
          case _ =>
        }
      case None =>
    }
    proctimeAttribute
  }

  /**
   * Validates a list of fields to be rowtime attributes.
   *
   * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
   */
  private def validateRowtimeAttributeDescriptors(
      rowtimeAttributeDescriptors: Seq[RowtimeAttributeDescriptor]) = {
    Preconditions.checkNotNull(rowtimeAttributeDescriptors,
      "List of rowtime attributes must not be null.")

    rowtimeAttributeDescriptors.foreach { desc =>
      val attr = desc.getAttributeName
      val tpe = schema.getFieldType(attr).asScala
      tpe match {
        case Some(dt) if dt != Types.SQL_TIMESTAMP() =>
          throw new ValidationException(
            s"Rowtime attribute '$attr' is not of type SQL_TIMESTAMP.")
        case None =>
          throw new ValidationException(
            s"Rowtime attribute '$attr' is not present in TableSchema.")
        case _ =>
      }
    }
    rowtimeAttributeDescriptors
  }
}
