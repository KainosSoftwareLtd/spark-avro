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

  private[avro] def convertFieldTypeToLogical(): Unit = {

  }

  private[avro] def convertToLogicalValue(item: Any): ByteBuffer = {
    val bigItem = item.asInstanceOf[java.math.BigDecimal]
    ByteBuffer.wrap(bigItem.unscaledValue().toByteArray)
  }

  private[avro] def toSqlType(datatype: LogicalType): Option[DecimalType] = {
    datatype match {
      case decimal: Decimal => Some(DecimalType(decimal.getPrecision, decimal.getScale))
      case _ => None
    }
  }

  private[avro] def toSql(logicalType: LogicalType,
                          item: Any,
                          schema: Schema): Option[BigDecimal] = {
    logicalType match {
      case decimalType: LogicalTypes.Decimal =>
        Some(decimalFromBytes(item.asInstanceOf[ByteBuffer], schema, decimalType))
      case _ => None
    }
  }

  private[avro] def decimalFromBytes(value: ByteBuffer,
                              schema: Schema,
                              decimalType: LogicalTypes.Decimal): BigDecimal = {
    val result = new Array[Byte](value.remaining)
    value.get(result)
    new BigDecimal(new BigInteger(result), decimalType.getScale)
  }
}