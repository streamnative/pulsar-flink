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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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
      in.read(ba)
      si.setSchema(ba)
    } else {
      si.setSchema(new Array[Byte](0))
    }
    si.setName(in.readUTF())
    si.setProperties(in.readObject().asInstanceOf[java.util.Map[String, String]])
    si.setType(SchemaType.valueOf(in.readInt()))
  }
}

private[pulsar] object SchemaUtils {

  private lazy val nullSchema = ASchema.create(ASchema.Type.NULL)

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

  case class TypeNullable(dataType: DataType, nullable: Boolean)

  def pulsarSourceSchema(si: SchemaInfo): StructType = {
    var mainSchema: ListBuffer[StructField] = ListBuffer.empty
    val typeNullable = si2SqlType(si)
    typeNullable.dataType match {
      case st: StructType =>
        mainSchema ++= st.fields
      case t =>
        mainSchema += StructField("value", t, nullable = typeNullable.nullable)
    }
    mainSchema ++= metaDataFields
    StructType(mainSchema)
  }

  def si2SqlType(si: SchemaInfo): TypeNullable = {
    si.getType match {
      case SchemaType.NONE => TypeNullable(BinaryType, nullable = false)
      case SchemaType.BOOLEAN => TypeNullable(BooleanType, nullable = false)
      case SchemaType.BYTES => TypeNullable(BinaryType, nullable = false)
      case SchemaType.DATE => TypeNullable(DateType, nullable = false)
      case SchemaType.STRING => TypeNullable(StringType, nullable = false)
      case SchemaType.TIMESTAMP => TypeNullable(TimestampType, nullable = false)
      case SchemaType.INT8 => TypeNullable(ByteType, nullable = false)
      case SchemaType.DOUBLE => TypeNullable(DoubleType, nullable = false)
      case SchemaType.FLOAT => TypeNullable(FloatType, nullable = false)
      case SchemaType.INT32 => TypeNullable(IntegerType, nullable = false)
      case SchemaType.INT64 => TypeNullable(LongType, nullable = false)
      case SchemaType.INT16 => TypeNullable(ShortType, nullable = false)
      case SchemaType.AVRO | SchemaType.JSON =>
        val avroSchema: ASchema =
          new ASchema.Parser().parse(new String(si.getSchema, StandardCharsets.UTF_8))
        avro2SqlType(avroSchema, Set.empty)
      case si =>
        throw new NotImplementedError(s"We do not support $si currently.")
    }
  }

  def avro2SqlType(avroSchema: ASchema, existingRecordNames: Set[String]): TypeNullable = {
    avroSchema.getType match {
      case INT =>
        avroSchema.getLogicalType match {
          case _: Date => TypeNullable(DateType, nullable = false)
          case _ => TypeNullable(IntegerType, nullable = false)
        }
      case STRING => TypeNullable(StringType, nullable = false)
      case BOOLEAN => TypeNullable(BooleanType, nullable = false)
      case BYTES | FIXED =>
        avroSchema.getLogicalType match {
          // For FIXED type, if the precision requires more bytes than fixed size, the logical
          // type will be null, which is handled by Avro library.
          case d: Decimal =>
            TypeNullable(DecimalType(d.getPrecision, d.getScale), nullable = false)
          case _ => TypeNullable(BinaryType, nullable = false)
        }

      case DOUBLE => TypeNullable(DoubleType, nullable = false)
      case FLOAT => TypeNullable(FloatType, nullable = false)
      case LONG =>
        avroSchema.getLogicalType match {
          case _: TimestampMillis | _: TimestampMicros =>
            TypeNullable(TimestampType, nullable = false)
          case _ => TypeNullable(LongType, nullable = false)
        }

      case ENUM => TypeNullable(StringType, nullable = false)

      case RECORD =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IncompatibleSchemaException(s"""
            |Found recursive reference in Avro schema, which can not be processed by Spark:
            |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>
          val typeNullable = avro2SqlType(f.schema(), newRecordNames)
          StructField(f.name, typeNullable.dataType, typeNullable.nullable)
        }

        TypeNullable(StructType(fields), nullable = false)

      case ARRAY =>
        val typeNullable: TypeNullable =
          avro2SqlType(avroSchema.getElementType, existingRecordNames)
        TypeNullable(
          ArrayType(typeNullable.dataType, containsNull = typeNullable.nullable),
          nullable = false)

      case MAP =>
        val typeNullable = avro2SqlType(avroSchema.getValueType, existingRecordNames)
        TypeNullable(
          MapType(StringType, typeNullable.dataType, valueContainsNull = typeNullable.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            avro2SqlType(remainingUnionTypes.head, existingRecordNames).copy(nullable = true)
          } else {
            avro2SqlType(ASchema.createUnion(remainingUnionTypes.asJava), existingRecordNames)
              .copy(nullable = true)
          }
        } else {
          avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(t1) =>
              avro2SqlType(avroSchema.getTypes.get(0), existingRecordNames)
            case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
              TypeNullable(LongType, nullable = false)
            case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
              TypeNullable(DoubleType, nullable = false)
            case _ =>
              // Convert complex unions to struct types where field names are member0, member1, etc.
              // This is consistent with the behavior when converting between Avro and Parquet.
              val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
                case (s, i) =>
                  val TypeNullable = avro2SqlType(s, existingRecordNames)
                  // All fields are nullable because only one of them is set at a time
                  StructField(s"member$i", TypeNullable.dataType, nullable = true)
              }

              TypeNullable(StructType(fields), nullable = false)
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

  def sqlType2PSchema(catalystType: DataType, nullable: Boolean = false): PSchema[_] = {

    catalystType match {
      case BooleanType => BooleanSchema.of()
      case BinaryType => BytesSchema.of()
      case DateType => DateSchema.of()
      case StringType => PSchema.STRING
      case TimestampType => TimestampSchema.of()
      case ByteType => ByteSchema.of()
      case DoubleType => DoubleSchema.of()
      case FloatType => FloatSchema.of()
      case IntegerType => IntSchema.of()
      case LongType => LongSchema.of()
      case ShortType => ShortSchema.of()
      case st: StructType => ASchema2PSchema(sqlType2ASchema(catalystType))
      case _ => throw new RuntimeException(s"$catalystType is not supported yet")
    }
  }

  // adapted from org.apache.spark.sql.avro.SchemaConverters#toAvroType
  def sqlType2ASchema(
      catalystType: DataType,
      nullable: Boolean = false,
      recordName: String = "topLevelRecord",
      nameSpace: String = ""): ASchema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(builder.longType())

      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      case StringType => builder.stringType()
      case BinaryType => builder.bytesType()

      case d: DecimalType =>
        val avroType = LogicalTypes.decimal(d.precision, d.scale)
        val fixedSize = minBytesForPrecision(d.precision)
        // Need to avoid naming conflict for the fixed fields
        val name = nameSpace match {
          case "" => s"$recordName.fixed"
          case _ => s"$nameSpace.$recordName.fixed"
        }
        avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))

      case ArrayType(et, containsNull) =>
        builder
          .array()
          .items(sqlType2ASchema(et, containsNull, recordName, nameSpace))

      case MapType(StringType, vt, valueContainsNull) =>
        builder
          .map()
          .values(sqlType2ASchema(vt, valueContainsNull, recordName, nameSpace))

      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val fieldAvroType =
            sqlType2ASchema(f.dataType, f.nullable, f.name, childNameSpace)
          fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
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

  val metaDataFields: Seq[StructField] = Seq(
    StructField(KEY_ATTRIBUTE_NAME, BinaryType),
    StructField(TOPIC_ATTRIBUTE_NAME, StringType),
    StructField(MESSAGE_ID_NAME, BinaryType),
    StructField(PUBLISH_TIME_NAME, TimestampType),
    StructField(EVENT_TIME_NAME, TimestampType)
  )

}
