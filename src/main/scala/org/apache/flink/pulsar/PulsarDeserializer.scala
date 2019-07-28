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
package org.apache.flink.pulsar

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.dataformat.{Decimal, GenericArray, GenericMap, GenericRow}
import org.apache.flink.table.types.{AtomicDataType, CollectionDataType, DataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.table.types.logical.{DecimalType, LogicalTypeRoot => LTR, RowType}

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

  import SchemaUtils._

  def deserialize(message: Message[_]): GenericRow = converter(message)

  private val converter: Message[_] => GenericRow = {

    schemaInfo.getType match {
      case SchemaType.AVRO =>
        val st = rootDataType.asInstanceOf[FieldsDataType]
        // TODO: can we reuse the row across records?
        val resultRow = new GenericRow(st.getFieldDataTypes.size() + metaDataFields.size)
        val fieldUpdater = new RowUpdater(resultRow)
        val avroSchema =
          new Schema.Parser().parse(new String(schemaInfo.getSchema, StandardCharsets.UTF_8))
        val writer = getRecordWriter(avroSchema, st, Nil)
        (msg: Message[_]) =>
        {
          val value = msg.getValue
          writer(fieldUpdater, value.asInstanceOf[GenericAvroRecord].getAvroRecord)
          writeMetadataFields(msg, resultRow)
          resultRow
        }

      case SchemaType.JSON =>
        val st = rootDataType.asInstanceOf[FieldsDataType]
        val resultRow = new GenericRow(st.getFieldDataTypes.size() + metaDataFields.size)
        val createParser = CreateJacksonParser.string _
        val rawParser = new JacksonRecordParser(rootDataType, parsedOptions)
        val parser = new FailureSafeRecordParser[String](
          (input, record) => rawParser.parse(input, createParser, _.toString, record),
          parsedOptions.parseMode,
          st)
        (msg: Message[_]) =>
        {
          val value = msg.getData
          parser.parse(new String(value, java.nio.charset.StandardCharsets.UTF_8), resultRow)
          writeMetadataFields(msg, resultRow)
          resultRow
        }

      case _ => // AtomicTypes
        val tmpRow = new GenericRow(1 + metaDataFields.size)
        val fieldUpdater = new RowUpdater(tmpRow)
        val writer = newAtomicWriter(rootDataType)
        (msg: Message[_]) =>
        {
          val value = msg.getValue
          writer(fieldUpdater, 0, value)
          writeMetadataFields(msg, tmpRow)
          tmpRow
        }
    }
  }

  def writeMetadataFields(message: Message[_], row: GenericRow): Unit = {
    val metaStartIdx = row.getArity - 5
    // key
    if (message.hasKey) {
      row.setField(metaStartIdx, message.getKeyBytes)
    } else {
      row.setNullAt(metaStartIdx)
    }
    // topic
    row.setField(metaStartIdx + 1, message.getTopicName)
    // messageId
    row.setField(metaStartIdx + 2, message.getMessageId.toByteArray)
    // publish time
    row.setLong(
      metaStartIdx + 3,
      DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getPublishTime)))
    // event time
    if (message.getEventTime > 0) {
      row.setLong(
        metaStartIdx + 4,
        DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getEventTime)))
    } else {
      row.setNullAt(metaStartIdx + 4)
    }
  }

  private def newAtomicWriter(dataType: DataType): (RowUpdater, Int, Any) => Unit = {
    assert(dataType.isInstanceOf[AtomicDataType])
    val tpe = dataType.getLogicalType.getTypeRoot
    tpe match {
      case LTR.TINYINT =>
        (updater, ordinal, value) =>
          updater.setByte(ordinal, value.asInstanceOf[Byte])

      case LTR.BOOLEAN =>
        (updater, ordinal, value) =>
          updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case LTR.INTEGER =>
        (updater, ordinal, value) =>
          updater.setInt(ordinal, value.asInstanceOf[Int])

      case LTR.DATE =>
        (updater, ordinal, value) =>
          updater.setInt(
            ordinal,
            (value.asInstanceOf[Date].getTime / DateTimeUtils.MILLIS_PER_DAY).toInt)

      case LTR.BIGINT =>
        (updater, ordinal, value) =>
          updater.setLong(ordinal, value.asInstanceOf[Long])

      case LTR.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        (updater, ordinal, value) =>
          updater.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[Timestamp]))

      case LTR.FLOAT =>
        (updater, ordinal, value) =>
          updater.setFloat(ordinal, value.asInstanceOf[Float])

      case LTR.DOUBLE =>
        (updater, ordinal, value) =>
          updater.setDouble(ordinal, value.asInstanceOf[Double])

      case LTR.SMALLINT =>
        (updater, ordinal, value) =>
          updater.setShort(ordinal, value.asInstanceOf[Short])

      case LTR.VARCHAR =>
        (updater, ordinal, value) =>
          val str = value.asInstanceOf[String]
          updater.set(ordinal, str)

      case LTR.BINARY =>
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

      case tpe =>
        throw new NotImplementedError(s"$tpe is not supported for the moment")
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
          updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case (INT, LTR.INTEGER) =>
        (updater, ordinal, value) =>
          updater.setInt(ordinal, value.asInstanceOf[Int])

      case (INT, LTR.DATE) =>
        (updater, ordinal, value) =>
          updater.setInt(ordinal, value.asInstanceOf[Int])

      case (LONG, LTR.BIGINT) =>
        (updater, ordinal, value) =>
          updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, LTR.TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
        avroType.getLogicalType match {
          case _: TimestampMillis =>
            (updater, ordinal, value) =>
              updater.setLong(ordinal, value.asInstanceOf[Long] * 1000)
          case _: TimestampMicros =>
            (updater, ordinal, value) =>
              updater.setLong(ordinal, value.asInstanceOf[Long])
          case other =>
            throw new IncompatibleSchemaException(
              s"Cannot convert Avro logical type ${other} to flink Timestamp type.")
        }

      case (FLOAT, LTR.FLOAT) =>
        (updater, ordinal, value) =>
          updater.setFloat(ordinal, value.asInstanceOf[Float])

      case (DOUBLE, LTR.DOUBLE) =>
        (updater, ordinal, value) =>
          updater.setDouble(ordinal, value.asInstanceOf[Double])

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

      case (BYTES, LTR.BINARY) =>
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
          val decimal = createDecimal(bigDecimal, d.getPrecision, d.getScale)
          updater.setDecimal(ordinal, decimal)

      case (BYTES, LTR.DECIMAL) =>
        val d = flinkType.getLogicalType.asInstanceOf[DecimalType]
        (updater, ordinal, value) =>
          val bigDecimal = decimalConversions.fromBytes(
            value.asInstanceOf[ByteBuffer],
            avroType,
            LogicalTypes.decimal(d.getPrecision, d.getScale))
          val decimal = createDecimal(bigDecimal, d.getPrecision, d.getScale)
          updater.setDecimal(ordinal, decimal)

      case (RECORD, LTR.ROW) =>
        val fieldsDataType = flinkType.asInstanceOf[FieldsDataType]
        val writeRecord = getRecordWriter(avroType, fieldsDataType, path)
        (updater, ordinal, value) =>
          val row = new GenericRow(fieldsDataType.getFieldDataTypes.size())
          writeRecord(new RowUpdater(row), value.asInstanceOf[GenericRecord])
          updater.set(ordinal, row)

      case (ARRAY, LTR.ARRAY) if flinkType.isInstanceOf[CollectionDataType] =>
        val elementType = flinkType.asInstanceOf[CollectionDataType].getElementDataType
        val containsNull = elementType.getLogicalType.isNullable
        val elementWriter = newWriter(avroType.getElementType, elementType, path)
        (updater, ordinal, value) =>
          val array = value.asInstanceOf[GenericData.Array[Any]]
          val len = array.size()
          val result = createArrayData(elementType, len)
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

          updater.set(ordinal, new GenericMap(map))

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
                    case l: java.lang.Long => updater.setLong(ordinal, l)
                    case i: java.lang.Integer => updater.setLong(ordinal, i.longValue())
                  }

              case Seq(a, b)
                  if Set(a, b) == Set(FLOAT, DOUBLE) && flinkType == DataTypes.DOUBLE() =>
                (updater, ordinal, value) =>
                  value match {
                    case null => updater.setNullAt(ordinal)
                    case d: java.lang.Double => updater.setDouble(ordinal, d)
                    case f: java.lang.Float => updater.setDouble(ordinal, f.doubleValue())
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
                      val row = new GenericRow(rt.getFieldCount)
                      val fieldUpdater = new RowUpdater(row)
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

  private def createDecimal(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      new Decimal(decimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      new Decimal(decimal, precision, scale)
    }
  }

  private def createArrayData(elementType: DataType, length: Int): GenericArray = {
    val tpe = elementType.getLogicalType.getTypeRoot
    tpe match {
      case LTR.BOOLEAN => new GenericArray(new Array[Boolean](length), length, true)
      case LTR.TINYINT => new GenericArray(new Array[Byte](length), length, true)
      case LTR.SMALLINT => new GenericArray(new Array[Short](length), length, true)
      case LTR.INTEGER => new GenericArray(new Array[Int](length), length, true)
      case LTR.BIGINT => new GenericArray(new Array[Long](length), length, true)
      case LTR.FLOAT => new GenericArray(new Array[Float](length), length, true)
      case LTR.DOUBLE => new GenericArray(new Array[Double](length), length, true)
      case _ => new GenericArray(new Array[Any](length))
    }
  }

  /**
   * A base interface for updating values inside flink data structure like `GenericRow` and
   * `GenericArray`.
   */
  sealed trait FlinkDataUpdater {
    def set(ordinal: Int, value: Any): Unit

    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: GenericRow) extends FlinkDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.setField(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.getPrecision)
  }

  final class ArrayDataUpdater(array: GenericArray) extends FlinkDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.setObject(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      array.setDecimal(ordinal, value, value.getPrecision)
  }

}
