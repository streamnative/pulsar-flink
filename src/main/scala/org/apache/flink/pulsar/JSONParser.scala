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

import java.io.{ByteArrayOutputStream, CharConversionException}
import java.nio.charset.MalformedInputException
import java.sql.{Date, Timestamp}
import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Locale, TimeZone}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import com.fasterxml.jackson.core._
import javax.xml.bind.DatatypeConverter
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.types.{CollectionDataType, DataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.table.types.logical.{DecimalType, LogicalTypeRoot => LTR, RowType}
import org.apache.flink.types.Row

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
  // to a value in a field for `Row`.
  private type ValueConverter = JsonParser => AnyRef

  // `ValueConverter`s for the root schema for all fields in the schema
  private val rootConverter =
    makeStructRootConverter(schema.asInstanceOf[FieldsDataType])

  private val factory = new JsonFactory()
  options.setJacksonOptions(factory)

  private def makeStructRootConverter(
      st: FieldsDataType): (JsonParser, Row) => Row = {
    val fieldConverters =
      st.getFieldDataTypes.asScala
        .map { case (_, dt) => dt }.map(makeConverter).toArray
    (parser: JsonParser, row: Row) =>
      parseJsonToken[Row](parser, st) {
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

      case LTR.TIMESTAMP_WITHOUT_TIME_ZONE =>
        (parser: JsonParser) =>
          parseJsonToken[java.sql.Timestamp](parser, dataType) {
            case VALUE_STRING =>
              val stringValue = parser.getText
              // This one will lose microseconds parts.
              // See https://issues.apache.org/jira/browse/SPARK-10681.
              DateTimeUtils.toJavaTimestamp(Long.box {
                Try(options.timestampFormat.parse(stringValue).getTime * 1000L)
                  .getOrElse {
                    // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                    // compatibility.
                    DateTimeUtils.stringToTime(stringValue).getTime * 1000L
                  }
              })

            case VALUE_NUMBER_INT =>
              DateTimeUtils.toJavaTimestamp(parser.getLongValue * 1000000L)
          }

      case LTR.DATE =>
        (parser: JsonParser) =>
          parseJsonToken[java.sql.Date](parser, dataType) {
            case VALUE_STRING =>
              val stringValue = parser.getText
              // This one will lose microseconds parts.
              // See https://issues.apache.org/jira/browse/SPARK-10681.x
              DateTimeUtils.toJavaDate(Int.box {
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
              })
          }

      case LTR.BINARY =>
        (parser: JsonParser) =>
          parseJsonToken[Array[Byte]](parser, dataType) {
            case VALUE_STRING => parser.getBinaryValue
          }

      case LTR.DECIMAL =>
        val dt = dataType.asInstanceOf[DecimalType]
        (parser: JsonParser) =>
          parseJsonToken[java.math.BigDecimal](parser, dataType) {
            case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) =>
              parser.getDecimalValue
          }

      case LTR.ROW =>
        val rowType = dataType.getLogicalType.asInstanceOf[RowType]
        val types = dataType.asInstanceOf[FieldsDataType].getFieldDataTypes
        val fieldNames = rowType.getFieldNames.asScala

        val fieldConverters = fieldNames.map(types.get(_)).map(makeConverter).toArray
        (parser: JsonParser) =>
          parseJsonToken[Row](parser, dataType) {
            case START_OBJECT =>
              val record = new Row(rowType.getFieldCount)
              convertObject(parser, dataType, fieldConverters, record)
          }

      case LTR.ARRAY =>
        val et = dataType.asInstanceOf[CollectionDataType].getElementDataType
        val elementConverter = makeConverter(et)
        (parser: JsonParser) =>
          parseJsonToken[Array[Any]](parser, dataType) {
            case START_ARRAY => convertArray(parser, elementConverter)
          }

      case LTR.MAP =>
        val vt = dataType.asInstanceOf[KeyValueDataType].getValueDataType
        val valueConverter = makeConverter(vt)
        (parser: JsonParser) =>
          parseJsonToken[util.Map[AnyRef, AnyRef]](parser, dataType) {
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
      row: Row): Row = {
    schema match {
      case fdt: FieldsDataType =>
        val rowType = fdt.getLogicalType.asInstanceOf[RowType]
        val fieldNames = rowType.getFieldNames.asScala
        while (nextUntil(parser, JsonToken.END_OBJECT)) {
          val index = fieldNames.indexOf(parser.getCurrentName)
          if (index == -1) {
            parser.skipChildren()
          } else {
            row.setField(index, fieldConverters(index).apply(parser))
          }
        }
        row

      case _ => throw new RuntimeException("not a row type")
    }
  }

  /**
   * Parse an object as a Map, preserving all fields.
   */
  private def convertMap(
      parser: JsonParser,
      fieldConverter: ValueConverter): util.Map[AnyRef, AnyRef] = {
    val kvs = mutable.HashMap.empty[AnyRef, AnyRef]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      kvs += (parser.getCurrentName -> fieldConverter.apply(parser))
    }

    kvs.toMap.asJava
  }

  /**
   * Parse an object as a Array.
   */
  private def convertArray(parser: JsonParser, fieldConverter: ValueConverter): Array[Any] = {
    val values = mutable.ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += fieldConverter.apply(parser)
    }

    values.toArray
  }

  /**
   * Parse the JSON input to [[Row]].
   */
  def parse[T](
      record: T,
      createParser: (JsonFactory, T) => JsonParser,
      row: Row): Row = {
    try {
      Utils.tryWithResource(createParser(factory, record)) { parser =>
        // a null first token is equivalent to testing for input.trim.isEmpty
        // but it works on any token stream and not just strings
        parser.nextToken() match {
          case null => new Row(0)
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
        throw BadRecordException(() => record.toString, () => None, e)
      case e: CharConversionException if options.encoding.isEmpty =>
        val msg =
          """JSON parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        throw BadRecordException(() => record.toString, () => None, wrappedCharException)
    }
  }
}

class FailureSafeRecordParser[IN](
    rawParser: (IN, Row) => Row,
    mode: ParseMode,
    schema: FieldsDataType) {

  def parse(input: IN, row: Row): Row = {
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
  // number of days between 1.1.1970 and 1.1.2001
  final val to2001 = -11323
  final val toYearZero = to2001 + 7304850

  // number of days in 400 years
  final val daysIn400Years: Int = 146097

  final val MICROS_PER_MILLIS = 1000L
  final val MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND
  final val MILLIS_PER_SECOND = 1000L

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

  /**
   * Returns the number of days since epoch from java.sql.Date.
   */
  def fromJavaDate(date: Date): Int = {
    millisToDays(date.getTime)
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

  // Converts Timestamp to string according to Hive TimestampWritable convention.
  def timestampToString(us: Long, timeZone: TimeZone): String = {
    val ts = toJavaTimestamp(us)
    val timestampString = ts.toString
    val timestampFormat = getThreadLocalTimestampFormat(timeZone)
    val formatted = timestampFormat.format(ts)

    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
  }

  // `SimpleDateFormat` is not thread-safe.
  private val threadLocalTimestampFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    }
  }

  def getThreadLocalTimestampFormat(timeZone: TimeZone): DateFormat = {
    val sdf = threadLocalTimestampFormat.get()
    sdf.setTimeZone(timeZone)
    sdf
  }

  /**
   * Returns a java.sql.Timestamp from number of micros since epoch.
   */
  def toJavaTimestamp(us: Long): Timestamp = {
    // setNanos() will overwrite the millisecond part, so the milliseconds should be
    // cut off at seconds
    var seconds = us / MICROS_PER_SECOND
    var micros = us % MICROS_PER_SECOND
    // setNanos() can not accept negative value
    if (micros < 0) {
      micros += MICROS_PER_SECOND
      seconds -= 1
    }
    val t = new Timestamp(seconds * 1000)
    t.setNanos(micros.toInt * 1000)
    t
  }

  /**
   * Returns a java.sql.Date from number of days since epoch.
   */
  def toJavaDate(daysSinceEpoch: Int): Date = {
    new Date(daysToMillis(daysSinceEpoch))
  }

  // reverse of millisToDays
  def daysToMillis(days: Int): Long = {
    daysToMillis(days, defaultTimeZone())
  }

  def daysToMillis(days: Int, timeZone: TimeZone): Long = {
    val millisLocal = days.toLong * MILLIS_PER_DAY
    millisLocal - getOffsetFromLocalMillis(millisLocal, timeZone)
  }

  /**
   * Lookup the offset for given millis seconds since 1970-01-01 00:00:00 in given timezone.
   * TODO: Improve handling of normalization differences.
   * TODO: Replace with JSR-310 or similar system - see SPARK-16788
   */
  private def getOffsetFromLocalMillis(millisLocal: Long, tz: TimeZone): Long = {
    var guess = tz.getRawOffset
    // the actual offset should be calculated based on milliseconds in UTC
    val offset = tz.getOffset(millisLocal - guess)
    if (offset != guess) {
      guess = tz.getOffset(millisLocal - offset)
      if (guess != offset) {
        // fallback to do the reverse lookup using java.sql.Timestamp
        // this should only happen near the start or end of DST
        val days = Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
        val year = getYear(days)
        val month = getMonth(days)
        val day = getDayOfMonth(days)

        var millisOfDay = (millisLocal % MILLIS_PER_DAY).toInt
        if (millisOfDay < 0) {
          millisOfDay += MILLIS_PER_DAY.toInt
        }
        val seconds = (millisOfDay / 1000L).toInt
        val hh = seconds / 3600
        val mm = seconds / 60 % 60
        val ss = seconds % 60
        val ms = millisOfDay % 1000
        val calendar = Calendar.getInstance(tz)
        calendar.set(year, month - 1, day, hh, mm, ss)
        calendar.set(Calendar.MILLISECOND, ms)
        guess = (millisLocal - calendar.getTimeInMillis()).toInt
      }
    }
    guess
  }

  /**
   * Returns the year value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getYear(date: Int): Int = {
    getYearAndDayInYear(date)._1
  }

  /**
   * Calculates the year and the number of the day in the year for the given
   * number of days. The given days is the number of days since 1.1.1970.
   *
   * The calculation uses the fact that the period 1.1.2001 until 31.12.2400 is
   * equals to the period 1.1.1601 until 31.12.2000.
   */
  private[this] def getYearAndDayInYear(daysSince1970: Int): (Int, Int) = {
    // add the difference (in days) between 1.1.1970 and the artificial year 0 (-17999)
    var  daysSince1970Tmp = daysSince1970
    // Since Julian calendar was replaced with the Gregorian calendar,
    // the 10 days after Oct. 4 were skipped.
    // (1582-10-04) -141428 days since 1970-01-01
    if (daysSince1970 <= -141428) {
      daysSince1970Tmp -= 10
    }
    val daysNormalized = daysSince1970Tmp + toYearZero
    val numOfQuarterCenturies = daysNormalized / daysIn400Years
    val daysInThis400 = daysNormalized % daysIn400Years + 1
    val (years, dayInYear) = numYears(daysInThis400)
    val year: Int = (2001 - 20000) + 400 * numOfQuarterCenturies + years
    (year, dayInYear)
  }

  /**
   * Calculates the number of years for the given number of days. This depends
   * on a 400 year period.
   * @param days days since the beginning of the 400 year period
   * @return (number of year, days in year)
   */
  private[this] def numYears(days: Int): (Int, Int) = {
    val year = days / 365
    val boundary = yearBoundary(year)
    if (days > boundary) (year, days - boundary) else (year - 1, days - yearBoundary(year - 1))
  }

  /**
   * Return the number of days since the start of 400 year period.
   * The second year of a 400 year period (year 1) starts on day 365.
   */
  private[this] def yearBoundary(year: Int): Int = {
    year * 365 + ((year / 4 ) - (year / 100) + (year / 400))
  }

  /**
   * Returns the month value for the given date. The date is expressed in days
   * since 1.1.1970. January is month 1.
   */
  def getMonth(date: Int): Int = {
    var (year, dayInYear) = getYearAndDayInYear(date)
    if (isLeapYear(year)) {
      if (dayInYear == 60) {
        return 2
      } else if (dayInYear > 60) {
        dayInYear = dayInYear - 1
      }
    }

    if (dayInYear <= 31) {
      1
    } else if (dayInYear <= 59) {
      2
    } else if (dayInYear <= 90) {
      3
    } else if (dayInYear <= 120) {
      4
    } else if (dayInYear <= 151) {
      5
    } else if (dayInYear <= 181) {
      6
    } else if (dayInYear <= 212) {
      7
    } else if (dayInYear <= 243) {
      8
    } else if (dayInYear <= 273) {
      9
    } else if (dayInYear <= 304) {
      10
    } else if (dayInYear <= 334) {
      11
    } else {
      12
    }
  }

  /**
   * Returns the 'day of month' value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getDayOfMonth(date: Int): Int = {
    var (year, dayInYear) = getYearAndDayInYear(date)
    if (isLeapYear(year)) {
      if (dayInYear == 60) {
        return 29
      } else if (dayInYear > 60) {
        dayInYear = dayInYear - 1
      }
    }

    if (dayInYear <= 31) {
      dayInYear
    } else if (dayInYear <= 59) {
      dayInYear - 31
    } else if (dayInYear <= 90) {
      dayInYear - 59
    } else if (dayInYear <= 120) {
      dayInYear - 90
    } else if (dayInYear <= 151) {
      dayInYear - 120
    } else if (dayInYear <= 181) {
      dayInYear - 151
    } else if (dayInYear <= 212) {
      dayInYear - 181
    } else if (dayInYear <= 243) {
      dayInYear - 212
    } else if (dayInYear <= 273) {
      dayInYear - 243
    } else if (dayInYear <= 304) {
      dayInYear - 273
    } else if (dayInYear <= 334) {
      dayInYear - 304
    } else {
      dayInYear - 334
    }
  }

  private[this] def isLeapYear(year: Int): Boolean = {
    (year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0)
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
  partialResult: () => Option[Row],
  cause: Throwable) extends Exception(cause)
