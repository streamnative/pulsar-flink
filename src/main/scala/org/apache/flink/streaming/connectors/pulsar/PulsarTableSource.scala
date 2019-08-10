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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.pulsar.Logging
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableSchema, Types, ValidationException}
import org.apache.flink.table.sources.{DefinedFieldMapping, DefinedProctimeAttribute, DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.utils.TableConnectorUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

case class PulsarTableSource(
    properties: Properties,
    proctimeAttribute: Option[String],
    rowtimeAttributeDescriptors: Seq[RowtimeAttributeDescriptor],
    fieldMapping: Option[Map[String, String]])
    extends StreamTableSource[Row]
    with DefinedProctimeAttribute
    with DefinedRowtimeAttributes
    with DefinedFieldMapping
    with Logging {

  validateProctimeAttribute(proctimeAttribute)
  validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors)

  var schema: TableSchema = _
  var returnType: TypeInformation[Row] = _

  override def getDataStream(env: StreamExecutionEnvironment): DataStream[Row] = {
    val source = new FlinkPulsarSource(properties)
    val rowDT = source.inferredSchema
    val rt = rowDT.getLogicalType.asInstanceOf[RowType]
    val fieldTypes = rt.getFieldNames.asScala.map(rowDT.getFieldDataTypes.get(_))

    schema = TableSchema.builder.fields(
      rt.getFieldNames.toArray(new Array[String](0)), fieldTypes.toArray).build()

    returnType = source.getProducedType

    env.addSource(source).name(explainSource)
  }

  override def getProctimeAttribute: String = proctimeAttribute.getOrElse(null)

  override def getRowtimeAttributeDescriptors: ju.List[RowtimeAttributeDescriptor] =
    rowtimeAttributeDescriptors.asJava

  override def getFieldMapping: ju.Map[String, String] = fieldMapping.getOrElse(null).asJava

  override def getTableSchema: TableSchema = schema

  override def getReturnType: TypeInformation[Row] = returnType

  override def explainSource: String =
    TableConnectorUtils.generateRuntimeName(this.getClass, schema.getFieldNames)

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
