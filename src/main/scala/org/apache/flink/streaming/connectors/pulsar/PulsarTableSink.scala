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

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.{TableSchema, ValidationException}
import org.apache.flink.table.sinks.{AppendStreamTableSink, TableSink}
import org.apache.flink.table.utils.TableConnectorUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

case class PulsarTableSink(
  schema: TableSchema,
  properties: Properties) extends AppendStreamTableSink[Row] {

  Preconditions.checkNotNull(schema, "Schema must not be null.")
  Preconditions.checkNotNull(properties, "Properties must not be null.")

  override def emitDataStream(dataStream: DataStream[Row]): Unit = {
    consumeDataStream(dataStream)
  }

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    val sink = new FlinkPulsarRowSink(schema.toRowDataType, properties)
    dataStream.addSink(sink).name(TableConnectorUtils.generateRuntimeName(getClass, getFieldNames))
  }

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    if (!fieldNames.sameElements(getFieldNames) || !fieldTypes.sameElements(getFieldTypes)) {
      throw new ValidationException("Reconfiguration with different fields is not allowed. " +
        s"Expected: $getFieldNames / $getFieldTypes, but was: $fieldNames / $fieldTypes")
    }
    this
  }

  override def getTableSchema: TableSchema = schema

  override def getOutputType: TypeInformation[Row] = schema.toRowType

  override def getFieldNames: Array[String] = schema.getFieldNames

  override def getFieldTypes: Array[TypeInformation[_]] = schema.getFieldTypes
}
