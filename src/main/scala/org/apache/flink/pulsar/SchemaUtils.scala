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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.types.{AtomicDataType, CollectionDataType, DataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.table.types.logical.{DecimalType, LogicalTypeRoot, RowType}

import org.apache.pulsar.client.admin.{PulsarAdmin, PulsarAdminException}
import org.apache.pulsar.client.api.{Schema => PSchema}
import org.apache.pulsar.client.api.schema.{GenericRecord, GenericSchema}
import org.apache.pulsar.client.impl.schema._
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import org.apache.pulsar.shade.org.apache.avro.{LogicalTypes, Schema => ASchema, SchemaBuilder}
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes.{Date, Decimal, TimestampMicros, TimestampMillis}
import org.apache.pulsar.shade.org.apache.avro.Schema.Type._

class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

class SchemaInfoSerializable(var si: SchemaInfo) extends Externalizable {

  def this() = this(null) // For deserialization only

  override def writeExternal(out: ObjectOutput): Unit = {
    val schema = si.getSchema
    if (schema.length == 0) {
      out.writeInt(0)
    } else {
      out.writeInt(schema.length)
      out.write(schema)
    }

    out.writeUTF(si.getName)
    out.writeObject(si.getProperties)
    out.writeInt(si.getType.getValue)
  }

  override def readExternal(in: ObjectInput): Unit = {
    si = new SchemaInfo()
    val len = in.readInt()
    if (len > 0) {
      val ba = new Array[Byte](len)
      in.readFully(ba)
      si.setSchema(ba)
    } else {
      si.setSchema(new Array[Byte](0))
    }
    si.setName(in.readUTF())
    si.setProperties(in.readObject().asInstanceOf[java.util.Map[String, String]])
    si.setType(SchemaType.valueOf(in.readInt()))
  }
}

object SchemaUtils {

  private lazy val nullSchema = ASchema.create(ASchema.Type.NULL)

  def toTableSchema(schema: FieldsDataType): TableSchema = {
    val rt = schema.getLogicalType.asInstanceOf[RowType]
    val fieldTypes = rt.getFieldNames.asScala.map(schema.getFieldDataTypes.get(_))

    TableSchema.builder.fields(
      rt.getFieldNames.toArray(new Array[String](0)), fieldTypes.toArray).build()
  }

  def uploadPulsarSchema(admin: PulsarAdmin, topic: String, schemaInfo: SchemaInfo): Unit = {
    assert(schemaInfo != null, "schemaInfo shouldn't be null")

    val existingSchema = try {
      admin.schemas().getSchemaInfo(TopicName.get(topic).toString)
    } catch {
      case e: PulsarAdminException if e.getStatusCode == 404 =>
        null
      case e: Throwable =>
        throw new RuntimeException(
          s"Failed to get schema information for ${TopicName.get(topic).toString}",
          e)
    }

    if (existingSchema == null) {
      val pl = new PostSchemaPayload()
      pl.setType(schemaInfo.getType.name())
      pl.setSchema(new String(schemaInfo.getSchema, UTF_8))
      pl.setProperties(schemaInfo.getProperties)
      try {
        admin.schemas().createSchema(TopicName.get(topic).toString, pl)
      } catch {
        case e: PulsarAdminException if e.getStatusCode == 404 =>
          throw new RuntimeException(
            s"Create schema for ${TopicName.get(topic).toString} got 404")
        case e: Throwable =>
          throw new RuntimeException(
            s"Failed to create schema for ${TopicName.get(topic).toString}",
            e)
      }
    } else if (existingSchema.equals(schemaInfo) || compatibleSchema(existingSchema, schemaInfo)) {
      // no need to upload again
    } else {
      throw new RuntimeException("Writing to a topic which have incompatible schema")
    }
  }

  def compatibleSchema(x: SchemaInfo, y: SchemaInfo): Boolean = {
    (x.getType, y.getType) match {
      // None and bytes are compatible
      case (SchemaType.NONE, SchemaType.BYTES) => true
      case (SchemaType.BYTES, SchemaType.NONE) => true
      case _ => false
    }
  }

  def emptySchemaInfo(): SchemaInfo = {
    SchemaInfo.builder().name("empty").`type`(SchemaType.NONE).schema(new Array[Byte](0)).build()
  }

  def getPSchema(schemaInfo: SchemaInfo): PSchema[_] = schemaInfo.getType match {
    case SchemaType.BOOLEAN =>
      BooleanSchema.of()
    case SchemaType.INT8 =>
      ByteSchema.of()
    case SchemaType.INT16 =>
      ShortSchema.of()
    case SchemaType.INT32 =>
      IntSchema.of()
    case SchemaType.INT64 =>
      LongSchema.of()
    case SchemaType.STRING =>
      PSchema.STRING
    case SchemaType.FLOAT =>
      FloatSchema.of()
    case SchemaType.DOUBLE =>
      DoubleSchema.of()
    case SchemaType.BYTES =>
      BytesSchema.of()
    case SchemaType.DATE =>
      DateSchema.of()
    case SchemaType.TIME =>
      TimeSchema.of()
    case SchemaType.TIMESTAMP =>
      TimestampSchema.of()
    case SchemaType.NONE =>
      BytesSchema.of()
    case SchemaType.AVRO | SchemaType.JSON =>
      GenericSchemaImpl.of(schemaInfo)
    case _ =>
      throw new IllegalArgumentException(
        "Retrieve schema instance from schema info for type '" +
          schemaInfo.getType + "' is not supported yet")
  }

  def pulsarSourceSchema(si: SchemaInfo): FieldsDataType = {
    var mainSchema: ListBuffer[DataTypes.Field] = ListBuffer.empty
    val typeNullable = si2SqlType(si)
    typeNullable match {
      case st: FieldsDataType =>
        val rt = st.getLogicalType.asInstanceOf[RowType]
        val fields = rt.getFieldNames.asScala.map { case name =>
          DataTypes.FIELD(name, st.getFieldDataTypes.get(name))
        }
        mainSchema ++= fields
      case t =>
        mainSchema += DataTypes.FIELD("value", t)
    }
    mainSchema ++= metaDataFields
    DataTypes.ROW(mainSchema: _*).asInstanceOf[FieldsDataType]
  }

  // TODO: Flink type system could not handle nullability correctly.
  // it would convert DataType from/to TypeInformation
  // TypeInformation could not tell nullability
  def si2SqlType(si: SchemaInfo): DataType = {
    si.getType match {
      case SchemaType.NONE => DataTypes.BYTES()// .notNull()
      case SchemaType.BOOLEAN => DataTypes.BOOLEAN()// .notNull()
      case SchemaType.BYTES => DataTypes.BYTES()// .notNull()
      case SchemaType.DATE => DataTypes.DATE()// .notNull()
      case SchemaType.STRING => DataTypes.STRING()// .notNull()
      case SchemaType.TIMESTAMP =>
        DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp])// .notNull()
      case SchemaType.INT8 => DataTypes.TINYINT()// .notNull()
      case SchemaType.DOUBLE => DataTypes.DOUBLE()// .notNull()
      case SchemaType.FLOAT => DataTypes.FLOAT()// .notNull()
      case SchemaType.INT32 => DataTypes.INT()// .notNull()
      case SchemaType.INT64 => DataTypes.BIGINT()// .notNull()
      case SchemaType.INT16 => DataTypes.SMALLINT()// .notNull()
      case SchemaType.AVRO | SchemaType.JSON =>
        val avroSchema: ASchema =
          new ASchema.Parser().parse(new String(si.getSchema, StandardCharsets.UTF_8))
        avro2SqlType(avroSchema, Set.empty)
      case si =>
        throw new NotImplementedError(s"We do not support $si currently.")
    }
  }

  def avro2SqlType(avroSchema: ASchema, existingRecordNames: Set[String]): DataType = {
    avroSchema.getType match {
      case INT =>
        avroSchema.getLogicalType match {
          case _: Date => DataTypes.DATE()// .notNull()
          case _ => DataTypes.INT()// .notNull()
        }
      case STRING => DataTypes.STRING()// .notNull()
      case BOOLEAN => DataTypes.BOOLEAN()// .notNull()
      case BYTES | FIXED =>
        avroSchema.getLogicalType match {
          // For FIXED type, if the precision requires more bytes than fixed size, the logical
          // type will be null, which is handled by Avro library.
          case d: Decimal =>
            DataTypes.DECIMAL(d.getPrecision, d.getScale)// .notNull()
          case _ => DataTypes.BYTES()// .notNull()
        }

      case DOUBLE => DataTypes.DOUBLE()// .notNull()
      case FLOAT => DataTypes.FLOAT()// .notNull()
      case LONG =>
        avroSchema.getLogicalType match {
          case _: TimestampMillis | _: TimestampMicros =>
            DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp])// .notNull()
          case _ => DataTypes.BIGINT()// .notNull()
        }

      case ENUM => DataTypes.STRING()// .notNull()

      case RECORD =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IncompatibleSchemaException(s"""
            |Found recursive reference in Avro schema, which can not be processed by Flink:
            |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>
          val typeNullable = avro2SqlType(f.schema(), newRecordNames)
          DataTypes.FIELD(f.name, typeNullable)
        }
        DataTypes.ROW(fields: _*)// .notNull()

      case ARRAY =>
        val typeNullable: DataType = avro2SqlType(avroSchema.getElementType, existingRecordNames)
        DataTypes.ARRAY(typeNullable)// .notNull()

      case MAP =>
        val typeNullable = avro2SqlType(avroSchema.getValueType, existingRecordNames)
        DataTypes.MAP(DataTypes.STRING(), typeNullable)// .notNull()

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            avro2SqlType(remainingUnionTypes.head, existingRecordNames).nullable()
          } else {
            avro2SqlType(
              ASchema.createUnion(remainingUnionTypes.asJava), existingRecordNames).nullable()
          }
        } else {
          avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(t1) =>
              avro2SqlType(avroSchema.getTypes.get(0), existingRecordNames)
            case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
              DataTypes.BIGINT()// .notNull()
            case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
              DataTypes.DOUBLE()// .notNull()
            case _ =>
              // Convert complex unions to struct types where field names are member0, member1, etc.
              // This is consistent with the behavior when converting between Avro and Parquet.
              val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
                case (s, i) =>
                  val typeNullable = avro2SqlType(s, existingRecordNames)
                  // All fields are nullable because only one of them is set at a time
                  DataTypes.FIELD(s"member$i", typeNullable)
              }

              DataTypes.ROW(fields: _*)
          }
        }
      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  def ASchema2PSchema(aschema: ASchema): GenericSchema[GenericRecord] = {
    val schema = aschema.toString.getBytes(StandardCharsets.UTF_8)
    val si = new SchemaInfo()
    si.setName("Avro")
    si.setSchema(schema)
    si.setType(SchemaType.AVRO)
    PSchema.generic(si)
  }

  def sqlType2PSchema(flinkType: DataType): PSchema[_] = {
    flinkType match {
      case _: AtomicDataType =>
        val tpe = flinkType.getLogicalType.getTypeRoot
        tpe match {
          case LogicalTypeRoot.BOOLEAN => BooleanSchema.of()
          case LogicalTypeRoot.BINARY => BytesSchema.of()
          case LogicalTypeRoot.DATE => DateSchema.of()
          case LogicalTypeRoot.VARCHAR => PSchema.STRING
          case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE => TimestampSchema.of()
          case LogicalTypeRoot.TINYINT => ByteSchema.of()
          case LogicalTypeRoot.DOUBLE => DoubleSchema.of()
          case LogicalTypeRoot.FLOAT => FloatSchema.of()
          case LogicalTypeRoot.INTEGER => IntSchema.of()
          case LogicalTypeRoot.BIGINT => LongSchema.of()
          case LogicalTypeRoot.SMALLINT => ShortSchema.of()
          case _ => throw new RuntimeException(s"$flinkType is not supported yet")
        }

      case row: FieldsDataType =>
        ASchema2PSchema(sqlType2ASchema(flinkType))
    }
  }

  def sqlType2ASchema(
      flinkType: DataType,
      nullable: Boolean = false,
      recordName: String = "topLevelRecord",
      nameSpace: String = ""): ASchema = {
    val builder = SchemaBuilder.builder()

    val tpe = flinkType.getLogicalType.getTypeRoot

    val schema: ASchema = flinkType match {
      case _: AtomicDataType =>
        tpe match {
          case LogicalTypeRoot.BOOLEAN => builder.booleanType()
          case LogicalTypeRoot.TINYINT | LogicalTypeRoot.SMALLINT | LogicalTypeRoot.INTEGER =>
            builder.intType()
          case LogicalTypeRoot.BIGINT => builder.longType()
          case LogicalTypeRoot.DATE =>
            LogicalTypes.date().addToSchema(builder.intType())
          case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
            LogicalTypes.timestampMicros().addToSchema(builder.longType())

          case LogicalTypeRoot.FLOAT => builder.floatType()
          case LogicalTypeRoot.DOUBLE => builder.doubleType()
          case LogicalTypeRoot.VARCHAR => builder.stringType()
          case LogicalTypeRoot.BINARY => builder.bytesType()

          case LogicalTypeRoot.DECIMAL =>
            val dt = flinkType.asInstanceOf[DecimalType]
            val avroType = LogicalTypes.decimal(dt.getPrecision, dt.getScale)
            val fixedSize = minBytesForPrecision(dt.getPrecision)
            // Need to avoid naming conflict for the fixed fields
            val name = nameSpace match {
              case "" => s"$recordName.fixed"
              case _ => s"$nameSpace.$recordName.fixed"
            }
            avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))
          case e => throw new RuntimeException(s"$e not supported yet")
        }

      case cdt: CollectionDataType =>
        if (tpe == LogicalTypeRoot.ARRAY) {
          val eType = cdt.getElementDataType
          builder
            .array()
            .items(sqlType2ASchema(eType, eType.getLogicalType.isNullable, recordName, nameSpace))
        } else {
          throw new IncompatibleSchemaException("Pulsar only support collection as array")
        }

      case kvt: KeyValueDataType =>
        val kt = kvt.getKeyDataType
        val vt = kvt.getValueDataType
        if (!kt.isInstanceOf[AtomicDataType] ||
          kt.getLogicalType.getTypeRoot != LogicalTypeRoot.VARCHAR) {
          throw new IncompatibleSchemaException("Pulsar only support string key map")
        }
        builder
          .map()
          .values(sqlType2ASchema(vt, vt.getLogicalType.isNullable, recordName, nameSpace))

      case fsdt: FieldsDataType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        fsdt.getFieldDataTypes.asScala.foreach { case (name, tpe) =>
          val fieldAvroType =
            sqlType2ASchema(tpe, tpe.getLogicalType.isNullable, name, childNameSpace)
          fieldsAssembler.name(name).`type`(fieldAvroType).noDefault()
        }
        fieldsAssembler.endRecord()


      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }

    if (nullable) {
      ASchema.createUnion(schema, nullSchema)
    } else {
      schema
    }
  }

  import PulsarOptions._

  val metaDataFields: Seq[DataTypes.Field] = Seq(
    DataTypes.FIELD(
      KEY_ATTRIBUTE_NAME,
      DataTypes.BYTES()),
    DataTypes.FIELD(
      TOPIC_ATTRIBUTE_NAME,
      DataTypes.STRING()),
    DataTypes.FIELD(
      MESSAGE_ID_NAME,
      DataTypes.BYTES()),
    DataTypes.FIELD(
      PUBLISH_TIME_NAME,
      DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp])),
    DataTypes.FIELD(
      EVENT_TIME_NAME,
      DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp]))
  )

  lazy val minBytesForPrecision = Array.tabulate[Int](39)(computeMinBytesForPrecision)

  private def computeMinBytesForPrecision(precision : Int) : Int = {
    var numBytes = 1
    while (math.pow(2.0, 8 * numBytes - 1) < math.pow(10.0, precision)) {
      numBytes += 1
    }
    numBytes
  }
}
