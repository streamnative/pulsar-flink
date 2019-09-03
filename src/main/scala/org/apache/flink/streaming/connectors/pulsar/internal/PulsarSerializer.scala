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
import java.util

import scala.collection.JavaConverters._

import org.apache.flink.table.types.{CollectionDataType, DataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.table.types.logical.{DecimalType, LogicalTypeRoot => LTR, RowType}
import org.apache.flink.types.Row

import org.apache.pulsar.client.api.schema.Field
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord
import org.apache.pulsar.shade.org.apache.avro.{LogicalTypes, Schema => ASchema}
import org.apache.pulsar.shade.org.apache.avro.Conversions.DecimalConversion
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.pulsar.shade.org.apache.avro.Schema.Type
import org.apache.pulsar.shade.org.apache.avro.Schema.Type._
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.pulsar.shade.org.apache.avro.util.Utf8

class PulsarSerializer(flinkType: DataType, nullable: Boolean) {

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private lazy val decimalConversions = new DecimalConversion()
  private val rootAvroType = SchemaUtils.sqlType2ASchema(flinkType)

  def getFields(aSchema: ASchema): util.List[Field] = {
    aSchema.getFields.asScala.map(f => new Field(f.name(), f.pos())).asJava
  }

  private val converter: Any => Any = {
    val actualAvroType: ASchema = resolveNullableType(rootAvroType, nullable)
    val baseConverter = flinkType match {
      case st: FieldsDataType =>
        newStructConverter(st, actualAvroType).asInstanceOf[Any => Any]
      case _ =>
        val converter = singleValueConverter(flinkType, actualAvroType)
        (data: Any) =>
          converter.apply(new PositionedGetter(data.asInstanceOf[Row]), 0)
    }
    if (nullable) { (data: Any) =>
      if (data == null) {
        null
      } else {
        baseConverter.apply(data)
      }
    } else {
      baseConverter
    }
  }

  private def singleValueConverter(dataType: DataType, avroType: ASchema): Converter = {
    val tpe = dataType.getLogicalType.getTypeRoot
    (tpe, avroType.getType) match {
      case (LTR.NULL, NULL) =>
        (getter, ordinal) =>
          null
      case (LTR.BOOLEAN, BOOLEAN) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Boolean]
      case (LTR.TINYINT, INT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Byte]
      case (LTR.SMALLINT, INT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Short]
      case (LTR.INTEGER, INT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Int]
      case (LTR.BIGINT, LONG) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Long]
      case (LTR.FLOAT, FLOAT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Float]
      case (LTR.DOUBLE, DOUBLE) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Double]
      case (LTR.VARCHAR, STRING) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[String]
      case (LTR.BINARY, BYTES) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Array[Byte]]
      case (LTR.DATE, INT) =>
        (getter, ordinal) =>
          getter.getField(ordinal)

      case (LTR.TIMESTAMP_WITHOUT_TIME_ZONE, LONG) =>
        avroType.getLogicalType match {
          case _: TimestampMillis | _: TimestampMicros =>
            (getter, ordinal) =>
              getter.getField(ordinal)
          case other =>
            throw new IncompatibleSchemaException(
              s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
        }

      case other =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst type $dataType to " +
            s"Avro type $avroType.")
    }
  }

  private type Converter = (PositionedGetter, Int) => Any

  private def newConverter(dataType: DataType, avroType: ASchema): Converter = {
    val tpe = dataType.getLogicalType.getTypeRoot
    (tpe, avroType.getType) match {
      case (LTR.NULL, NULL) =>
        (getter, ordinal) =>
          null
      case (LTR.BOOLEAN, BOOLEAN) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Boolean]
      case (LTR.TINYINT, INT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Byte].toInt
      case (LTR.SMALLINT, INT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Short].toInt
      case (LTR.INTEGER, INT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Int]
      case (LTR.BIGINT, LONG) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Long]
      case (LTR.FLOAT, FLOAT) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Float]
      case (LTR.DOUBLE, DOUBLE) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Double]

      case (LTR.DECIMAL, FIXED) =>
        val d = dataType.getLogicalType.asInstanceOf[DecimalType]
        if (avroType.getLogicalType == LogicalTypes.decimal(d.getPrecision, d.getScale)) {
          (getter, ordinal) =>
            val decimal = getter.getField(ordinal).asInstanceOf[java.math.BigDecimal]
            decimalConversions.toFixed(
              decimal,
              avroType,
              LogicalTypes.decimal(d.getPrecision, d.getScale))
        } else {
          throw new IncompatibleSchemaException(
            s"Cannot convert flink decimal type to Avro logical type")
        }

      case (LTR.DECIMAL, BYTES) =>
        val d = dataType.getLogicalType.asInstanceOf[DecimalType]
        if (avroType.getLogicalType == LogicalTypes.decimal(d.getPrecision, d.getScale)) {
          (getter, ordinal) =>
            val decimal = getter.getField(ordinal).asInstanceOf[java.math.BigDecimal]
          decimalConversions.toBytes(
            decimal,
            avroType,
            LogicalTypes.decimal(d.getPrecision, d.getScale))
        } else {
          throw new IncompatibleSchemaException(
            s"Cannot convert flink decimal type to Avro logical type")
        }

      case (LTR.VARBINARY, BYTES) =>
        (getter, ordinal) =>
          getter.getField(ordinal).asInstanceOf[Array[Byte]]

      case (LTR.BINARY, BYTES) =>
        (getter, ordinal) =>
          ByteBuffer.wrap(getter.getField(ordinal).asInstanceOf[Array[Byte]])
      case (LTR.DATE, INT) =>
        (getter, ordinal) =>
          DateTimeUtils.fromJavaDate(getter.getField(ordinal).asInstanceOf[java.sql.Date])

      case (LTR.TIMESTAMP_WITHOUT_TIME_ZONE, LONG) =>
        avroType.getLogicalType match {
          case _: TimestampMillis =>
            (getter, ordinal) =>
              DateTimeUtils.fromJavaTimestamp(
                getter.getField(ordinal).asInstanceOf[java.sql.Timestamp]) / 1000
          case _: TimestampMicros =>
            (getter, ordinal) =>
              DateTimeUtils.fromJavaTimestamp(
                getter.getField(ordinal).asInstanceOf[java.sql.Timestamp])
          case other =>
            throw new IncompatibleSchemaException(
              s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
        }

      case (LTR.VARCHAR, STRING) =>
        (getter, ordinal) =>
          new Utf8(getter.getField(ordinal).asInstanceOf[String])

      case (LTR.VARCHAR, ENUM) =>
        val enumSymbols: Set[String] = avroType.getEnumSymbols.asScala.toSet
        (getter, ordinal) =>
          val data = getter.getField(ordinal).asInstanceOf[String]
          if (!enumSymbols.contains(data)) {
            throw new IncompatibleSchemaException(
              "Cannot write \"" + data + "\" since it's not defined in enum \"" +
                enumSymbols.mkString("\", \"") + "\"")
          }
          new EnumSymbol(avroType, data)

      case (LTR.ARRAY, ARRAY) if dataType.isInstanceOf[CollectionDataType] =>
        val et = flinkType.asInstanceOf[CollectionDataType].getElementDataType
        val containsNull = et.getLogicalType.isNullable
        val elementConverter =
          newConverter(et, resolveNullableType(avroType.getElementType, containsNull))
        (getter, ordinal) =>
          {
            val arrayData = getter.getField(ordinal).asInstanceOf[Array[Any]]
            val len = arrayData.size
            val result = new Array[Any](len)
            var i = 0
            while (i < len) {
              if (containsNull && arrayData(i) == null) {
                result(i) = null
              } else {
                result(i) = elementConverter(new PositionedGetter(arrayData), i)
              }
              i += 1
            }
            // avro writer is expecting a Java Collection, so we convert it into
            // `ArrayList` backed by the specified array without data copying.
            java.util.Arrays.asList(result: _*)
          }

      case (LTR.MAP, MAP) if dataType.asInstanceOf[KeyValueDataType]
            .getKeyDataType.getLogicalType.getTypeRoot == LTR.VARCHAR =>

        val kvt = dataType.asInstanceOf[KeyValueDataType]
        val ktl = kvt.getKeyDataType.getLogicalType
        val vt = kvt.getValueDataType
        val vtl = kvt.getValueDataType.getLogicalType
        val valueContainsNull = vt.getLogicalType.isNullable

        val valueConverter =
          newConverter(vt, resolveNullableType(avroType.getValueType, valueContainsNull))

        (getter, ordinal) =>
          getter.getField(ordinal)

      case (LTR.ROW, RECORD) =>
        val st = dataType.asInstanceOf[FieldsDataType]
        val structConverter = newStructConverter(st, avroType)
        (getter, ordinal) =>
          structConverter(getter.getField(ordinal).asInstanceOf[Row]).getAvroRecord

      case other =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst type $dataType to " +
            s"Avro type $avroType.")
    }
  }

  private def newStructConverter(
      dataType: FieldsDataType,
      avroStruct: ASchema): Row => GenericAvroRecord = {
    if (avroStruct.getType != RECORD ||
      avroStruct.getFields.size() != dataType.getFieldDataTypes.size()) {
      throw new IncompatibleSchemaException(
        s"Cannot convert Catalyst type $dataType to " +
          s"Avro type $avroStruct.")
    }

    val fieldsType = dataType.getFieldDataTypes
    val fields = dataType.getLogicalType.asInstanceOf[RowType].getFields.asScala

    val fieldConverters = fields.map(f => fieldsType.get(f.getName))
      .zip(avroStruct.getFields.asScala).map {
        case (f1, f2) =>
          newConverter(f1, resolveNullableType(f2.schema(), f1.getLogicalType.isNullable()))
      }
    val numFields = fieldsType.size()
    row: Row =>
      val pSchema = SchemaUtils.ASchema2PSchema(avroStruct)
      val builder = pSchema.newRecordBuilder()

      var i = 0
      while (i < numFields) {
        if (row.getField(i) == null) {
          builder.set(pSchema.getFields.get(i), null)
        } else {
          builder.set(pSchema.getFields.get(i),
            fieldConverters(i).apply(new PositionedGetter(row), i))
        }
        i += 1
      }
      builder.build().asInstanceOf[GenericAvroRecord]
  }

  private def resolveNullableType(avroType: ASchema, nullable: Boolean): ASchema = {
    if (nullable && avroType.getType != NULL) {
      // avro uses union to represent nullable type.
      val fields = avroType.getTypes.asScala
      assert(fields.length == 2)
      val actualType = fields.filter(_.getType != Type.NULL)
      assert(actualType.length == 1)
      actualType.head
    } else {
      avroType
    }
  }
}

class PositionedGetter(array: Array[Any], row: Row) {

  def this(arr: Array[Any]) = this(arr, null)

  def this(row: Row) = this(null, row)

  val getter: Int => Any = if (array != null) array.apply else row.getField

  def getField(i: Int): Any = getter(i)

}
