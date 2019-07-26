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
package org.apache.flink.connectors.pulsar.common

import java.io.{ByteArrayOutputStream, CharConversionException}
import java.nio.charset.MalformedInputException
import java.sql.{Date, Timestamp}
import java.util.{Locale, TimeZone}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import com.fasterxml.jackson.core._
import javax.xml.bind.DatatypeConverter
import org.apache.flink.table.dataformat.{Decimal, GenericArray, GenericMap, GenericRow}
import org.apache.flink.table.types.{CollectionDataType, DataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.table.types.logical.{DecimalType, LogicalTypeRoot => LTR, RowType}

object CreateJacksonParser extends Serializable {
  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }
}

class JacksonRecordParser(schema: DataType, val options: JSONOptions) extends Logging {

  import JacksonUtils._
  import com.fasterxml.jackson.core.JsonToken._

  assert(schema.isInstanceOf[FieldsDataType])

  // A `ValueConverter` is responsible for converting a value from `JsonParser`
  // to a value in a field for `GenericRow`.
  private type ValueConverter = JsonParser => AnyRef

  // `ValueConverter`s for the root schema for all fields in the schema
  private val rootConverter =
    makeStructRootConverter(schema.asInstanceOf[FieldsDataType])

  private val factory = new JsonFactory()
  options.setJacksonOptions(factory)

  private def makeStructRootConverter(
      st: FieldsDataType): (JsonParser, GenericRow) => GenericRow = {
    val fieldConverters =
      st.getFieldDataTypes.asScala
        .map { case (_, dt) => dt }.map(makeConverter).toArray
    (parser: JsonParser, row: GenericRow) =>
      parseJsonToken[GenericRow](parser, st) {
        case START_OBJECT => convertObject(parser, st, fieldConverters, row)
        case START_ARRAY =>
          throw new IllegalStateException("Message should be a single JSON object")
      }
  }

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser`
   * to a value according to a desired schema.
   */
  def makeConverter(dataType: DataType): ValueConverter = {
    val tpe = dataType.getLogicalType.getTypeRoot

    tpe match {
      case LTR.BOOLEAN =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Boolean](parser, dataType) {
            case VALUE_TRUE => true
            case VALUE_FALSE => false
          }

      case LTR.TINYINT =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Byte](parser, dataType) {
            case VALUE_NUMBER_INT => parser.getByteValue
          }

      case LTR.SMALLINT =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Short](parser, dataType) {
            case VALUE_NUMBER_INT => parser.getShortValue
          }

      case LTR.INTEGER =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Integer](parser, dataType) {
            case VALUE_NUMBER_INT => parser.getIntValue
          }

      case LTR.BIGINT =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Long](parser, dataType) {
            case VALUE_NUMBER_INT => parser.getLongValue
          }

      case LTR.FLOAT =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Float](parser, dataType) {
            case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
              parser.getFloatValue

            case VALUE_STRING =>
              // Special case handling for NaN and Infinity.
              parser.getText match {
                case "NaN" => Float.NaN
                case "Infinity" => Float.PositiveInfinity
                case "-Infinity" => Float.NegativeInfinity
                case other =>
                  throw new RuntimeException(s"Cannot parse $other as FLOAT.")
              }
          }

      case LTR.DOUBLE =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Double](parser, dataType) {
            case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
              parser.getDoubleValue

            case VALUE_STRING =>
              // Special case handling for NaN and Infinity.
              parser.getText match {
                case "NaN" => Double.NaN
                case "Infinity" => Double.PositiveInfinity
                case "-Infinity" => Double.NegativeInfinity
                case other =>
                  throw new RuntimeException(s"Cannot parse $other as DOUBLE.")
              }
          }

      case LTR.VARCHAR =>
        (parser: JsonParser) =>
          parseJsonToken[String](parser, dataType) {
            case VALUE_STRING =>
              parser.getText

            case _ =>
              // Note that it always tries to convert
              // the data as string without the case of failure.
              val writer = new ByteArrayOutputStream()
              Utils.tryWithResource(factory.createGenerator(writer, JsonEncoding.UTF8)) {
                generator =>
                  generator.copyCurrentStructure(parser)
              }
              new String(writer.toByteArray, "UTF_8")
          }

      case LTR.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Long](parser, dataType) {
            case VALUE_STRING =>
              val stringValue = parser.getText
              // This one will lose microseconds parts.
              // See https://issues.apache.org/jira/browse/SPARK-10681.
              Long.box {
                Try(options.timestampFormat.parse(stringValue).getTime * 1000L)
                  .getOrElse {
                    // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                    // compatibility.
                    DateTimeUtils.stringToTime(stringValue).getTime * 1000L
                  }
              }

            case VALUE_NUMBER_INT =>
              parser.getLongValue * 1000000L
          }

      case LTR.DATE =>
        (parser: JsonParser) =>
          parseJsonToken[java.lang.Integer](parser, dataType) {
            case VALUE_STRING =>
              val stringValue = parser.getText
              // This one will lose microseconds parts.
              // See https://issues.apache.org/jira/browse/SPARK-10681.x
              Int.box {
                Try(DateTimeUtils.millisToDays(options.dateFormat.parse(stringValue).getTime))
                  .orElse {
                    // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                    // compatibility.
                    Try(DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(stringValue).getTime))
                  }
                  .getOrElse {
                    // In Spark 1.5.0, we store the data as number of days since epoch in string.
                    // So, we just convert it to Int.
                    stringValue.toInt
                  }
              }
          }

      case LTR.BINARY =>
        (parser: JsonParser) =>
          parseJsonToken[Array[Byte]](parser, dataType) {
            case VALUE_STRING => parser.getBinaryValue
          }

      case LTR.DECIMAL =>
        val dt = dataType.asInstanceOf[DecimalType]
        (parser: JsonParser) =>
          parseJsonToken[Decimal](parser, dataType) {
            case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) =>
              new Decimal(parser.getDecimalValue, dt.getPrecision, dt.getScale)
          }

      case LTR.ROW =>
        val rowType = dataType.getLogicalType.asInstanceOf[RowType]
        val types = dataType.asInstanceOf[FieldsDataType].getFieldDataTypes
        val fieldNames = rowType.getFieldNames.asScala

        val fieldConverters = fieldNames.map(types.get(_)).map(makeConverter).toArray
        (parser: JsonParser) =>
          parseJsonToken[GenericRow](parser, dataType) {
            case START_OBJECT =>
              val record = new GenericRow(rowType.getFieldCount)
              convertObject(parser, dataType, fieldConverters, record)
          }

      case LTR.ARRAY =>
        val et = dataType.asInstanceOf[CollectionDataType].getElementDataType
        val elementConverter = makeConverter(et)
        (parser: JsonParser) =>
          parseJsonToken[GenericArray](parser, dataType) {
            case START_ARRAY => convertArray(parser, elementConverter)
          }

      case LTR.MAP =>
        val vt = dataType.asInstanceOf[KeyValueDataType].getValueDataType
        val valueConverter = makeConverter(vt)
        (parser: JsonParser) =>
          parseJsonToken[GenericMap](parser, dataType) {
            case START_OBJECT => convertMap(parser, valueConverter)
          }

      case _ =>
        (parser: JsonParser) =>
          // Here, we pass empty `PartialFunction` so that this case can be
          // handled as a failed conversion. It will throw an exception as
          // long as the value is not null.
          parseJsonToken[AnyRef](parser, dataType)(PartialFunction.empty[JsonToken, AnyRef])
    }
  }

  /**
   * This method skips `FIELD_NAME`s at the beginning, and handles nulls ahead before trying
   * to parse the JSON token using given function `f`. If the `f` failed to parse and convert the
   * token, call `failedConversion` to handle the token.
   */
  private def parseJsonToken[R >: Null](parser: JsonParser, dataType: DataType)(
      f: PartialFunction[JsonToken, R]): R = {
    parser.getCurrentToken match {
      case FIELD_NAME =>
        // There are useless FIELD_NAMEs between START_OBJECT and END_OBJECT tokens
        parser.nextToken()
        parseJsonToken[R](parser, dataType)(f)

      case null | VALUE_NULL => null

      case other => f.applyOrElse(other, failedConversion(parser, dataType))
    }
  }

  /**
   * This function throws an exception for failed conversion, but returns null for empty string,
   * to guard the non string types.
   */
  private def failedConversion[R >: Null](
      parser: JsonParser,
      dataType: DataType): PartialFunction[JsonToken, R] = {
    case VALUE_STRING if parser.getTextLength < 1 =>
      // If conversion is failed, this produces `null` rather than throwing exception.
      // This will protect the mismatch of types.
      null

    case token =>
      // We cannot parse this token based on the given data type. So, we throw a
      // RuntimeException and this exception will be caught by `parse` method.
      throw new RuntimeException(
        s"Failed to parse a value for data type $dataType (current token: $token).")
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: JsonParser,
      schema: DataType,
      fieldConverters: Array[ValueConverter],
      row: GenericRow): GenericRow = {
    schema match {
      case fdt: FieldsDataType =>
        val rowType = fdt.getLogicalType.asInstanceOf[RowType]
        val fieldNames = rowType.getFieldNames.asScala
        val allFields = 0 to rowType.getFieldCount
        val nullFields = collection.mutable.Set(allFields: _*)
        while (nextUntil(parser, JsonToken.END_OBJECT)) {
          val index = fieldNames.indexOf(parser.getCurrentName)
          if (index == -1) {
            parser.skipChildren()
          } else {
            nullFields.remove(index)
            row.setField(index, fieldConverters(index).apply(parser))
          }
        }
        nullFields.foreach(row.setNullAt)
        row

      case _ => throw new RuntimeException("not a row type")
    }
  }

  /**
   * Parse an object as a Map, preserving all fields.
   */
  private def convertMap(parser: JsonParser, fieldConverter: ValueConverter): GenericMap = {
    val kvs = mutable.HashMap.empty[AnyRef, AnyRef]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      kvs += (parser.getCurrentName -> fieldConverter.apply(parser))
    }

    new GenericMap(kvs.toMap.asJava)
  }

  /**
   * Parse an object as a Array.
   */
  private def convertArray(parser: JsonParser, fieldConverter: ValueConverter): GenericArray = {
    val values = mutable.ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += fieldConverter.apply(parser)
    }

    new GenericArray(values.toArray)
  }

  /**
   * Parse the JSON input to [[GenericRow]].
   *
   * @param recordLiteral an optional function that will be used to generate
   *   the corrupt record text instead of record.toString
   */
  def parse[T](
      record: T,
      createParser: (JsonFactory, T) => JsonParser,
      recordLiteral: T => String,
      row: GenericRow): GenericRow = {
    try {
      Utils.tryWithResource(createParser(factory, record)) { parser =>
        // a null first token is equivalent to testing for input.trim.isEmpty
        // but it works on any token stream and not just strings
        parser.nextToken() match {
          case null => new GenericRow(0)
          case _ =>
            rootConverter.apply(parser, row) match {
              case null => throw new RuntimeException("Root converter returned null")
              case row => row
            }
        }
      }
    } catch {
      case e @ (_: RuntimeException | _: JsonProcessingException | _: MalformedInputException) =>
        // JSON parser currently doesn't support partial results for corrupted records.
        // For such records, all fields other than the meta fields are set to `null`.
        throw BadRecordException(() => recordLiteral(record), () => None, e)
      case e: CharConversionException if options.encoding.isEmpty =>
        val msg =
          """JSON parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        throw BadRecordException(() => recordLiteral(record), () => None, wrappedCharException)
    }
  }
}

class FailureSafeRecordParser[IN](
    rawParser: (IN, GenericRow) => GenericRow,
    mode: ParseMode,
    schema: FieldsDataType) {

  def parse(input: IN, row: GenericRow): GenericRow = {
    try {
      rawParser.apply(input, row)
    } catch {
      case e: BadRecordException =>
        mode match {
          case PermissiveMode =>
            row
          case DropMalformedMode =>
            null
          case FailFastMode =>
            throw new RuntimeException(
              "Malformed records are detected in record parsing. " +
                s"Parse Mode: ${FailFastMode.name}.",
              e.cause)
        }
    }
  }
}

object DateTimeUtils {
  final val SECONDS_PER_DAY = 60 * 60 * 24L
  final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L


  @tailrec
  def stringToTime(s: String): java.util.Date = {
    val indexOfGMT = s.indexOf("GMT")
    if (indexOfGMT != -1) {
      // ISO8601 with a weird time zone specifier (2000-01-01T00:00GMT+01:00)
      val s0 = s.substring(0, indexOfGMT)
      val s1 = s.substring(indexOfGMT + 3)
      // Mapped to 2000-01-01T00:00+01:00
      stringToTime(s0 + s1)
    } else if (!s.contains('T')) {
      // JDBC escape string
      if (s.contains(' ')) {
        Timestamp.valueOf(s)
      } else {
        Date.valueOf(s)
      }
    } else {
      DatatypeConverter.parseDateTime(s).getTime()
    }
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  def millisToDays(millisUtc: Long): Int = {
    millisToDays(millisUtc, defaultTimeZone())
  }

  def millisToDays(millisUtc: Long, timeZone: TimeZone): Int = {
    // SPARK-6785: use Math.floor so negative number of days (dates before 1970)
    // will correctly work as input for function toJavaDate(Int)
    val millisLocal = millisUtc + timeZone.getOffset(millisUtc)
    Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
  }

  def defaultTimeZone(): TimeZone = TimeZone.getDefault()

  /**
   * Returns the number of micros since epoch from java.sql.Timestamp.
   */
  def fromJavaTimestamp(t: Timestamp): Long = {
    if (t != null) {
      t.getTime() * 1000L + (t.getNanos().toLong / 1000) % 1000L
    } else {
      0L
    }
  }
}

object JacksonUtils {
  /**
   * Advance the parser until a null or a specific token is found
   */
  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }
}

sealed trait ParseMode {
  /**
   * String name of the parse mode.
   */
  def name: String
}

/**
 * This mode permissively parses the records.
 */
case object PermissiveMode extends ParseMode { val name = "PERMISSIVE" }

/**
 * This mode ignores the whole corrupted records.
 */
case object DropMalformedMode extends ParseMode { val name = "DROPMALFORMED" }

/**
 * This mode throws an exception when it meets corrupted records.
 */
case object FailFastMode extends ParseMode { val name = "FAILFAST" }

object ParseMode extends Logging {
  /**
   * Returns the parse mode from the given string.
   */
  def fromString(mode: String): ParseMode = mode.toUpperCase(Locale.ROOT) match {
    case PermissiveMode.name => PermissiveMode
    case DropMalformedMode.name => DropMalformedMode
    case FailFastMode.name => FailFastMode
    case _ =>
      logWarning(s"$mode is not a valid parse mode. Using ${PermissiveMode.name}.")
      PermissiveMode
  }
}

/**
 * Exception thrown when the underlying parser meet a bad record and can't parse it.
 * @param record a function to return the record that cause the parser to fail
 * @param partialResult a function that returns an optional row, which is the partial result of
 *                      parsing this bad record.
 * @param cause the actual exception about why the record is bad and can't be parsed.
 */
case class BadRecordException(
  record: () => String,
  partialResult: () => Option[GenericRow],
  cause: Throwable) extends Exception(cause)
