/*
 * Copyright 2014 Databricks
 *
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
package com.databricks.spark.avro

import java.nio.ByteBuffer
import java.util.HashMap

import scala.collection.JavaConversions._
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro._
import org.apache.avro.Schema.Type._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
 * versa.
 */
private object SchemaConverters {

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  private[avro] def toSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case INT => SchemaType(IntegerType, nullable = false)
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES =>
        lazy val decimalType = LogicalTypeConverters.toSqlType(avroSchema.getLogicalType)
        if(null != avroSchema.getLogicalType && decimalType.isDefined) {
          SchemaType(decimalType.get, nullable = false)
        } else {
          SchemaType(BinaryType, nullable = false)
        }
      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => SchemaType(LongType, nullable = false)
      case FIXED => SchemaType(BinaryType, nullable = false)
      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.map { f =>
          val schemaType = toSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.get(0)).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes)).copy(nullable = true)
          }
        } else avroSchema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case other => throw new UnsupportedOperationException(
            s"This mix of union types is not supported (see README): $other")
        }

      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  /**
  * This function converts sparkSQL StructType into avro schema.
  * This method uses the convertFieldType
  * converter method in order to do the conversion.
  */
  private[avro] def convertStructToAvro(structType: StructType,
                                        structName: String,
                                        recordNamespace: String): Schema = {
    val fields = structType.fields.map { structField =>
      val fieldSchema = convertFieldType(structField.dataType, structField.name, recordNamespace)

      if (structField.nullable) {
        new Schema.Field(structField.name, optional(fieldSchema), null, JsonProperties.NULL_VALUE)
      }
      else {
        new Schema.Field(structField.name, fieldSchema, null, null.asInstanceOf[Object])
      }
    }.toList

    Schema.createRecord(structName, null, recordNamespace, false, fields)
  }

  /**
  * This function is used to construct field schemas of the avro record.
  * The data type of each field is used to determine which field schema to create.
  */
  private[avro] def convertFieldType(dataType: DataType,
                                     structName: String,
                                     recordNamespace: String): Schema = {
    dataType match {
      case ByteType => Schema.create(Schema.Type.INT)
      case ShortType => Schema.create(Schema.Type.INT)
      case IntegerType => Schema.create(Schema.Type.INT)
      case LongType => Schema.create(Schema.Type.LONG)
      case FloatType => Schema.create(Schema.Type.FLOAT)
      case DoubleType => Schema.create(Schema.Type.DOUBLE)
      case decimalType: DecimalType =>
        LogicalTypeConverters.convertDataTypeToLogical(decimalType)
      case StringType => Schema.create(Schema.Type.STRING)
      case BinaryType => Schema.create(Schema.Type.BYTES)
      case BooleanType => Schema.create(Schema.Type.BOOLEAN)
      case TimestampType => Schema.create(Schema.Type.LONG)

      case ArrayType(elementType, _) =>
        if (dataType.asInstanceOf[ArrayType].containsNull) {
          Schema.createArray(optional(convertFieldType(elementType, structName, recordNamespace)))
        }
        else {
          Schema.createArray(convertFieldType(elementType, structName, recordNamespace))
        }

      case MapType(StringType, valueType, _) =>
        if (dataType.asInstanceOf[MapType].valueContainsNull) {
          Schema.createMap(optional(convertFieldType(valueType, structName, recordNamespace)))
        }
        else {
          Schema.createMap(convertFieldType(valueType, structName, recordNamespace))
        }

      case structType: StructType =>
        convertStructToAvro(structType, structName, recordNamespace)

      case other => throw new UnsupportedOperationException(s"Unexpected type $dataType.")
    }
  }

  /**
  * Creates a Union of null and the original field schema to enable optional schemas
  */
  private[avro] def optional(original: Schema): Schema = {
    Schema.createUnion(Schema.create(Schema.Type.NULL), original)
  }

  /**
   * Returns a function that is used to convert avro types to their
   * corresponding sparkSQL representations.
   */
  private[avro] def createConverterToSQL(schema: Schema): Any => Any = {
    schema.getType match {
      // Avro strings are in Utf8, so we have to call toString on them
      case STRING | ENUM => (item: Any) => if (item == null) null else item.toString
      case INT | BOOLEAN | DOUBLE | FLOAT | LONG => identity
      // Byte arrays are reused by avro, so we have to make a copy of them.
      case FIXED => (item: Any) => if (item == null) {
        null
      } else {
        item.asInstanceOf[Fixed].bytes().clone()
      }
      case BYTES => (item: Any) => if (item == null) {
        null
      } else {
        lazy val decimalValue = LogicalTypeConverters.
          toSqlValue(
            schema.getLogicalType,
            item,
            schema
          )
        if(null != schema.getLogicalType && decimalValue.isDefined) {
          decimalValue.get
        } else {
          val bytes = item.asInstanceOf[ByteBuffer]
          val javaBytes = new Array[Byte](bytes.remaining)
          bytes.get(javaBytes)
          javaBytes
        }
      }
      case RECORD =>
        val fieldConverters = schema.getFields.map(f => createConverterToSQL(f.schema))
        (item: Any) => if (item == null) {
          null
        } else {
          val record = item.asInstanceOf[GenericRecord]
          val converted = new Array[Any](fieldConverters.size)
          var idx = 0
          while (idx < fieldConverters.size) {
            converted(idx) = fieldConverters.apply(idx)(record.get(idx))
            idx += 1
          }
          Row.fromSeq(converted.toSeq)
        }
      case ARRAY =>
        val elementConverter = createConverterToSQL(schema.getElementType)
        (item: Any) => if (item == null) {
          null
        } else {
          item.asInstanceOf[GenericData.Array[Any]].map(elementConverter)
        }
      case MAP =>
        val valueConverter = createConverterToSQL(schema.getValueType)
        (item: Any) => if (item == null) {
          null
        } else {
          item.asInstanceOf[HashMap[Any, Any]].map(x => (x._1.toString, valueConverter(x._2))).toMap
        }
      case UNION =>
        if (schema.getTypes.exists(_.getType == NULL)) {
          val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            createConverterToSQL(remainingUnionTypes.get(0))
          } else {
            createConverterToSQL(Schema.createUnion(remainingUnionTypes))
          }
        } else schema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            (item: Any) => {
              item match {
                case l: Long => l
                case i: Int => i.toLong
                case null => null
              }
            }
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            (item: Any) => {
              item match {
                case d: Double => d
                case f: Float => f.toDouble
                case null => null
              }
            }
          case other => throw new UnsupportedOperationException(
            s"This mix of union types is not supported (see README): $other")
        }
      case other => throw new UnsupportedOperationException(s"invalid avro type: $other")
    }
  }
}
