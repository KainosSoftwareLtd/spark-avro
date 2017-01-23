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
package com.kainos.spark.avro

import java.math.{BigDecimal, BigInteger}
import java.nio.ByteBuffer

import org.apache.avro.{Schema, _}
import org.apache.avro.LogicalTypes.Decimal
import org.apache.spark.sql.types._

/**
  * This object contains methods that are used to convert
  * Avro Logical types to Spark SQL schemas and vice versa.
  */

private object LogicalTypeConverters {

  /**
  * Converts a supported Spark SQL datatype to the correct avro schema before applying the
  * logical type schema
  */
  private[avro] def convertDataTypeToLogical(dataType: DataType): Schema = {
    dataType match {
      case decimalType: DecimalType =>
        val bytesSchema = Schema.create(Schema.Type.BYTES)
        LogicalTypes.decimal(decimalType.precision, decimalType.scale).addToSchema(bytesSchema)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported logical type conversion $other")
    }
  }

  /**
    * Converts a supported Spark SQL value to the correct avro value
    * Currently only supports BigDecimal to ByteBuffer conversions
    */
  private[avro] def convertToLogicalValue(item: Any): ByteBuffer = {
    val bigItem = item.asInstanceOf[java.math.BigDecimal]
    ByteBuffer.wrap(bigItem.unscaledValue().toByteArray)
  }

  /**
    * Converts a supported Avro LogicalType to SparkSQL Datatype
    */
  private[avro] def toSqlType(datatype: LogicalType): Option[DecimalType] = {
    datatype match {
      case decimal: Decimal => Some(DecimalType(decimal.getPrecision, decimal.getScale))
      case _ => None
    }
  }

  /**
    * Converts a supported Avro LogicalType value to SparkSQL value
    */
  private[avro] def toSqlValue(logicalType: LogicalType,
                               item: Any,
                               schema: Schema): Option[BigDecimal] = {
    logicalType match {
      case decimalType: LogicalTypes.Decimal =>
        Some(decimalFromBytes(item.asInstanceOf[ByteBuffer], schema, decimalType))
      case _ => None
    }
  }

  /**
    * Handles the BigDecimal to Bytes conversion for the Avro Decimal type
    */
  private[avro] def decimalFromBytes(value: ByteBuffer,
                              schema: Schema,
                              decimalType: LogicalTypes.Decimal): BigDecimal = {
    val result = new Array[Byte](value.remaining)
    value.get(result)
    new BigDecimal(new BigInteger(result), decimalType.getScale)
  }
}
