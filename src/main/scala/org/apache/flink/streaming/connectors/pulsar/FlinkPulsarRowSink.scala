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

import scala.collection.JavaConverters._

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.pulsar.internal.{
  DateTimeUtils,
  PulsarSerializer,
  SchemaUtils
}
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.{DataType, FieldsDataType}
import org.apache.flink.table.types.logical.{LogicalTypeRoot => LTR, RowType}
import org.apache.flink.types.Row

import org.apache.pulsar.client.api.Schema

class FlinkPulsarRowSink(schema: DataType, parameters: Properties)
    extends FlinkPulsarSinkBase[Row](parameters, DummyTopicKeyExtractor) {

  val (valueSchema, metaProj, valueProj, valIsStruct) = createProjection

  @transient lazy val serializer = new PulsarSerializer(valueSchema, false)

  override def pulsarSchema[R](element: Option[R]): Schema[_] =
    SchemaUtils.sqlType2PSchema(valueSchema)

  /**
   * Writes the given value to the sink. This function is called for every record.
   *
   * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
   * {@code default} method for backward compatibility with the old-style method only.
   *
   * @param value   The input record.
   * @param context Additional context about the input record.
   * @throws Exception This method may throw exceptions.
   *                   Throwing an exception will cause the operation
   *                   to fail and may trigger recovery.
   */
  override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {
    checkForErrors()

    val metaRow = metaProj(value)
    val valueRow = valueProj(value)
    val v = serializer.serialize(valueRow)

    val topic =
      if (forcedTopic) topicName else metaRow.getField(0).asInstanceOf[String]
    val key = metaRow.getField(1).asInstanceOf[String]
    val eventTime = metaRow.getField(2).asInstanceOf[java.sql.Timestamp]

    if (topic == null) {
      if (doFailOnWrite) {
        throw new NullPointerException(
          s"null topic present in the data. Use the " +
            s"$TOPIC_SINGLE option for setting a topic."
        )
      }
      return
    }

    val builder = getProducer[Any](topic, value).newMessage().value(v)

    if (null != key) {
      builder.keyBytes(key.getBytes)
    }

    if (null != eventTime) {
      val et = DateTimeUtils.fromJavaTimestamp(eventTime)
      if (et > 0) {
        builder.eventTime(et)
      }
    }

    if (doFlushOnCheckpoint) {
      pendingRecordsLock.synchronized {
        pendingRecords += 1
      }
    }
    builder.sendAsync().whenComplete(sendCallback)
  }

  type Projection = Row => Row

  private def createProjection = {
    val metas = new Array[Int](3) // topic, key, eventTime

    val fdt = schema.asInstanceOf[FieldsDataType]
    val fdtm = fdt.getFieldDataTypes

    val length = fdt.getFieldDataTypes.size()
    val rowFields = fdt.getLogicalType.asInstanceOf[RowType].getFields.asScala
    val name2tpe: Map[String, (LTR, Int)] = rowFields.zipWithIndex.map {
      case (f, i) => (f.getName, (f.getType.getTypeRoot, i))
    }.toMap

    // topic
    name2tpe.get(TOPIC_ATTRIBUTE_NAME) match {
      case Some((tpe, i)) =>
        if (tpe == LTR.VARCHAR) {
          metas(0) = i
        } else {
          throw new IllegalStateException(
            TOPIC_ATTRIBUTE_NAME +
              s"attribute unsupported type $tpe. $TOPIC_ATTRIBUTE_NAME " +
              s"must be a string"
          )
        }
      case None =>
        if (!forcedTopic) {
          throw new IllegalStateException(
            s"topic option required when no " +
              s"'$TOPIC_ATTRIBUTE_NAME' attribute is present"
          )
        }
        metas(0) = -1
    }
    // key
    name2tpe.get(KEY_ATTRIBUTE_NAME) match {
      case Some(t) =>
        if (t._1 == LTR.VARBINARY) {
          metas(1) = t._2
        } else {
          throw new IllegalStateException(
            s"$KEY_ATTRIBUTE_NAME attribute unsupported type ${t._1}"
          )
        }
      case None => metas(1) = -1
    }
    // eventTime
    name2tpe.get(EVENT_TIME_NAME) match {
      case Some(t) =>
        if (t._1 == LTR.TIMESTAMP_WITHOUT_TIME_ZONE) {
          metas(2) = t._2
        } else {
          throw new IllegalStateException(
            s"$EVENT_TIME_NAME attribute unsupported type ${t._1}"
          )
        }
      case None => metas(2) = -1
    }

    val values =
      (0 until length).toSet.filterNot(metas.toSet.contains(_)).toSeq.sorted

    val valuesNameAndType = values.map { i =>
      val name = rowFields(i).getName
      val dt = fdtm.get(name)
      (name, dt)
    }

    val valueType =
      if (values.size == 1) {
        valuesNameAndType(0)._2
      } else {
        val fields =
          rowFields.filterNot(f => META_FIELD_NAMES.contains(f.getName)).map {
            case f =>
              val fieldName = f.getName
              DataTypes.FIELD(fieldName, fdtm.get(fieldName))
          }
        DataTypes.ROW(fields: _*)
      }

    val metaProj: Projection = { origin: Row =>
      val result = new Row(3)
      metas.zipWithIndex.foreach {
        case (slot, i) =>
          if (slot != -1) result.setField(i, origin.getField(slot))
      }
      result
    }

    val valueProj: Projection = { origin: Row =>
      val result = new Row(values.size)
      values.zipWithIndex.foreach {
        case (slot, i) =>
          result.setField(i, origin.getField(slot))
      }
      result
    }

    (valueType, metaProj, valueProj, values.size == 1)
  }

}
