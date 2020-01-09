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
package org.apache.flink.streaming.connectors.pulsar.internal

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.ZoneId
import java.util.Date

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils.IncompatibleSchemaException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.{CollectionDataType, DataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.table.types.logical.{DecimalType, RowType, LogicalTypeRoot => LTR}
import org.apache.flink.types.Row
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import org.apache.pulsar.shade.org.apache.avro.{LogicalTypes, Schema}
import org.apache.pulsar.shade.org.apache.avro.Conversions.DecimalConversion
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.pulsar.shade.org.apache.avro.Schema.Type.{ARRAY, BOOLEAN, BYTES, DOUBLE, ENUM, FIXED, FLOAT, INT, LONG, MAP, NULL, RECORD, STRING, UNION}
import org.apache.pulsar.shade.org.apache.avro.generic.{GenericData, GenericFixed, GenericRecord}
import org.apache.pulsar.shade.org.apache.avro.util.Utf8

class PulsarDeserializer(schemaInfo: SchemaInfo, parsedOptions: JSONOptions) {

  private lazy val decimalConversions = new DecimalConversion()

  val rootDataType: DataType = SchemaUtils.si2SqlType(schemaInfo)

  import PulsarOptions._

  def deserialize(message: Message[_]): Row = converter(message)

  private val converter: Message[_] => Row = {

    schemaInfo.getType match {
      case SchemaType.AVRO =>
        val st = rootDataType.asInstanceOf[FieldsDataType]
        val fieldsNum = st.getFieldDataTypes.size() + META_FIELD_NAMES.size
        val fieldUpdater = new RowUpdater
        val avroSchema =
          new Schema.Parser().parse(new String(schemaInfo.getSchema, StandardCharsets.UTF_8))
        val writer = getRecordWriter(avroSchema, st, Nil)
        (msg: Message[_]) =>
        {
          val resultRow = new Row(fieldsNum)
          fieldUpdater.setRow(resultRow)
          val value = msg.getValue
          writer(fieldUpdater, value.asInstanceOf[GenericAvroRecord].getAvroRecord)
          writeMetadataFields(msg, resultRow)
          resultRow
        }

      case SchemaType.JSON =>
        val st = rootDataType.asInstanceOf[FieldsDataType]
        val createParser: (JsonFactory, String) => JsonParser = CreateJacksonParser.string _
        val rawParser = new JacksonRecordParser(rootDataType, parsedOptions)
        val parser = new FailureSafeRecordParser[String](
          (input, record) => rawParser.parse(input, createParser, record),
          parsedOptions.getParseMode,
          st)
        (msg: Message[_]) =>
        {
          val resultRow = new Row(st.getFieldDataTypes.size() + META_FIELD_NAMES.size)
          val value = msg.getData
          parser.parse(new String(value, java.nio.charset.StandardCharsets.UTF_8), resultRow)
          writeMetadataFields(msg, resultRow)
          resultRow
        }

      case _ => // AtomicTypes
        val fieldUpdater = new RowUpdater
        val writer = newAtomicWriter(rootDataType)
        (msg: Message[_]) =>
        {
          val tmpRow = new Row(1 + META_FIELD_NAMES.size)
          fieldUpdater.setRow(tmpRow)
          val value = msg.getValue
          writer(fieldUpdater, 0, value)
          writeMetadataFields(msg, tmpRow)
          tmpRow
        }
    }
  }

  def writeMetadataFields(message: Message[_], row: Row): Unit = {
    val metaStartIdx = row.getArity - 5
    // key
    if (message.hasKey) {
      row.setField(metaStartIdx, message.getKeyBytes)
    } else {
      row.setField(metaStartIdx, null)
    }
    // topic
    row.setField(metaStartIdx + 1, message.getTopicName)
    // messageId
    row.setField(metaStartIdx + 2, message.getMessageId.toByteArray)
    // publish time
    row.setField(metaStartIdx + 3, new Timestamp(message.getPublishTime))
    // event time
    if (message.getEventTime > 0) {
      row.setField(metaStartIdx + 4, new Timestamp(message.getEventTime))
    } else {
      row.setField(metaStartIdx + 4, null)
    }
  }

  private def newAtomicWriter(dataType: DataType): (RowUpdater, Int, Any) => Unit = {
    val tpe = dataType.getLogicalType.getTypeRoot
    tpe match {

      case LTR.DATE =>
        (updater, ordinal, value) =>
          updater.set(
            ordinal,
            value.asInstanceOf[Date].toInstant.atZone(ZoneId.systemDefault()).toLocalDate)

      case other =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value)
    }
  }

  private def newWriter(
    avroType: Schema,
    flinkType: DataType,
    path: List[String]): (FlinkDataUpdater, Int, Any) => Unit = {

    val tpe = flinkType.getLogicalType.getTypeRoot

    (avroType.getType, tpe) match {
      case (NULL, LTR.NULL) =>
        (updater, ordinal, _) =>
          updater.setNullAt(ordinal)

      case (BOOLEAN, LTR.BOOLEAN) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value)

      case (INT, LTR.INTEGER) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value)

      case (LONG, LTR.BIGINT) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value)

      case (FLOAT, LTR.FLOAT) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value)

      case (DOUBLE, LTR.DOUBLE) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value)

      case (INT, LTR.DATE) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, DateTimeUtils.toJavaDate(value.asInstanceOf[Int]))

      case (LONG, LTR.TIMESTAMP_WITHOUT_TIME_ZONE) =>
        avroType.getLogicalType match {
          case _: TimestampMillis =>
            (updater, ordinal, value) =>
              updater.set(ordinal, DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long] * 1000))
          case _: TimestampMicros =>
            (updater, ordinal, value) =>
              updater.set(ordinal, DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long]))
          case other =>
            throw new IncompatibleSchemaException(
              s"Cannot convert Avro logical type ${other} to flink Timestamp type.")
        }

      case (STRING, LTR.VARCHAR) =>
        (updater, ordinal, value) =>
          val str = value match {
            case s: String => s
            case s: Utf8 =>
              val bytes = new Array[Byte](s.getByteLength)
              System.arraycopy(s.getBytes, 0, bytes, 0, s.getByteLength)
              new String(bytes, "UTF-8")
          }
          updater.set(ordinal, str)

      case (ENUM, LTR.VARCHAR) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value.toString)

      case (FIXED, LTR.BINARY) =>
        (updater, ordinal, value) =>
          updater.set(ordinal, value.asInstanceOf[GenericFixed].bytes().clone())

      case (BYTES, LTR.VARBINARY) =>
        (updater, ordinal, value) =>
          val bytes = value match {
            case b: ByteBuffer =>
              val bytes = new Array[Byte](b.remaining)
              b.get(bytes)
              bytes
            case b: Array[Byte] => b
            case other => throw new RuntimeException(s"$other is not a valid avro binary.")
          }
          updater.set(ordinal, bytes)

      case (FIXED, LTR.DECIMAL) =>
        val d = flinkType.getLogicalType.asInstanceOf[DecimalType]
        (updater, ordinal, value) =>
          val bigDecimal = decimalConversions.fromFixed(
            value.asInstanceOf[GenericFixed],
            avroType,
            LogicalTypes.decimal(d.getPrecision, d.getScale))
          updater.set(ordinal, bigDecimal)

      case (BYTES, LTR.DECIMAL) =>
        val d = flinkType.getLogicalType.asInstanceOf[DecimalType]
        (updater, ordinal, value) =>
          val bigDecimal = decimalConversions.fromBytes(
            value.asInstanceOf[ByteBuffer],
            avroType,
            LogicalTypes.decimal(d.getPrecision, d.getScale))
          updater.set(ordinal, bigDecimal)

      case (RECORD, LTR.ROW) =>
        val fieldsDataType = flinkType.asInstanceOf[FieldsDataType]
        val writeRecord = getRecordWriter(avroType, fieldsDataType, path)
        (updater, ordinal, value) =>
          val row = new Row(fieldsDataType.getFieldDataTypes.size())
          val ru = new RowUpdater
          ru.setRow(row)
          writeRecord(ru, value.asInstanceOf[GenericRecord])
          updater.set(ordinal, row)

      case (ARRAY, LTR.ARRAY) if flinkType.isInstanceOf[CollectionDataType] =>
        val elementType = flinkType.asInstanceOf[CollectionDataType].getElementDataType
        val containsNull = elementType.getLogicalType.isNullable
        val elementWriter = newWriter(avroType.getElementType, elementType, path)
        (updater, ordinal, value) =>
          val array = value.asInstanceOf[GenericData.Array[Any]]
          val len = array.size()
          val result = new Array[Any](len)
          val elementUpdater = new ArrayDataUpdater(result)

          var i = 0
          while (i < len) {
            val element = array.get(i)
            if (element == null) {
              if (!containsNull) {
                throw new RuntimeException(
                  s"Array value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
              } else {
                elementUpdater.setNullAt(i)
              }
            } else {
              elementWriter(elementUpdater, i, element)
            }
            i += 1
          }

          updater.set(ordinal, result)

      case (MAP, LTR.MAP) if
          flinkType.asInstanceOf[KeyValueDataType]
            .getKeyDataType.getLogicalType.getTypeRoot == LTR.VARCHAR =>

        val kvt = flinkType.asInstanceOf[KeyValueDataType]
        val kt = kvt.getKeyDataType
        val vt = kvt.getValueDataType
        val valueContainsNull = vt.getLogicalType.isNullable

        (updater, ordinal, value) =>
          val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]

          val iter = map.entrySet().iterator()
          var i = 0
          while (iter.hasNext) {
            val entry = iter.next()
            assert(entry.getKey != null)
            if (entry.getValue == null) {
              if (!valueContainsNull) {
                throw new RuntimeException(
                  s"Map value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
              }
            }
            i += 1
          }

          updater.set(ordinal, map)

      case (UNION, _) =>
        val allTypes = avroType.getTypes.asScala
        val nonNullTypes = allTypes.filter(_.getType != NULL)
        if (nonNullTypes.nonEmpty) {
          if (nonNullTypes.length == 1) {
            newWriter(nonNullTypes.head, flinkType, path)
          } else {
            nonNullTypes.map(_.getType) match {
              case Seq(a, b) if Set(a, b) == Set(INT, LONG) && flinkType == DataTypes.BIGINT() =>
                (updater, ordinal, value) =>
                  value match {
                    case null => updater.setNullAt(ordinal)
                    case l: java.lang.Long => updater.set(ordinal, l)
                    case i: java.lang.Integer => updater.set(ordinal, i.longValue())
                  }

              case Seq(a, b)
                  if Set(a, b) == Set(FLOAT, DOUBLE) && flinkType == DataTypes.DOUBLE() =>
                (updater, ordinal, value) =>
                  value match {
                    case null => updater.setNullAt(ordinal)
                    case d: java.lang.Double => updater.set(ordinal, d)
                    case f: java.lang.Float => updater.set(ordinal, f.doubleValue())
                  }

              case _ =>
                tpe match {
                  case LTR.ROW if
                    flinkType.getLogicalType.asInstanceOf[RowType]
                      .getFieldCount == nonNullTypes.size =>

                    val feildsType = flinkType.asInstanceOf[FieldsDataType].getFieldDataTypes

                    val rt = flinkType.getLogicalType.asInstanceOf[RowType]
                    val fieldWriters = nonNullTypes
                      .zip(rt.getFieldNames.asScala)
                      .map {
                        case (schema, field) =>
                          newWriter(schema, feildsType.get(field), path :+ field)
                      }
                      .toArray
                    (updater, ordinal, value) =>
                    {
                      val row = new Row(rt.getFieldCount)
                      val fieldUpdater = new RowUpdater()
                      fieldUpdater.setRow(row)
                      val i = GenericData.get().resolveUnion(avroType, value)
                      fieldWriters(i)(fieldUpdater, i, value)
                      updater.set(ordinal, row)
                    }

                  case _ =>
                    throw new IncompatibleSchemaException(
                      s"Cannot convert Avro to flink because schema at path " +
                        s"${path.mkString(".")} is not compatible " +
                        s"(avroType = $avroType, sqlType = $flinkType).\n")
                }
            }
          }
        } else { (updater, ordinal, value) =>
          updater.setNullAt(ordinal)
        }

      case _ =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Avro to flink because schema at path ${path.mkString(".")} " +
            s"is not compatible (avroType = $avroType, sqlType = $flinkType).\n")
    }
  }

  private def getRecordWriter(
    avroType: Schema,
    sqlType: FieldsDataType,
    path: List[String]): (RowUpdater, GenericRecord) => Unit = {
    val validFieldIndexes = ArrayBuffer.empty[Int]
    val fieldWriters = ArrayBuffer.empty[(RowUpdater, Any) => Unit]

    val length = sqlType.getFieldDataTypes.size()
    val fields = sqlType.getLogicalType.asInstanceOf[RowType].getFields
    val fieldsType = sqlType.getFieldDataTypes
    var i = 0
    while (i < length) {
      val sqlField = fields.get(i)
      val avroField = avroType.getField(sqlField.getName)
      if (avroField != null) {
        validFieldIndexes += avroField.pos()

        val baseWriter = newWriter(
          avroField.schema(), fieldsType.get(sqlField.getName), path :+ sqlField.getName)
        val ordinal = i
        val fieldWriter = (fieldUpdater: RowUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        fieldWriters += fieldWriter
      } else if (!sqlField.getType.isNullable) {
        throw new IncompatibleSchemaException(
          s"""
             |Cannot find non-nullable field ${path
            .mkString(".")}.${sqlField.getName} in Avro schema.
             |Source Avro schema: $avroType.
             |Target flink type: $sqlType.
           """.stripMargin)
      }
      i += 1
    }

    (fieldUpdater, record) =>
    {
      var i = 0
      while (i < validFieldIndexes.length) {
        fieldWriters(i)(fieldUpdater, record.get(validFieldIndexes(i)))
        i += 1
      }
    }
  }

  /**
   * A base interface for updating values inside flink data structure like `Row` and
   * `GenericArray`.
   */
  sealed trait FlinkDataUpdater {
    def set(ordinal: Int, value: Any): Unit

    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
  }

  final class RowUpdater extends FlinkDataUpdater {

    var row: Row = null

    def setRow(currentRow: Row): Unit = {
      row = currentRow
    }

    override def set(ordinal: Int, value: Any): Unit = row.setField(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = row.setField(ordinal, null)
  }

  final class ArrayDataUpdater(array: Array[Any]) extends FlinkDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array(ordinal) = value

    override def setNullAt(ordinal: Int): Unit = array(ordinal) = null
  }

}
