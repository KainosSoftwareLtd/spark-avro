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

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.spark.sql.types.DecimalType
import org.scalatest.FunSuite

class LogicalTypeConvertersSuite extends FunSuite {

  test("conversions from bytes toSql value of Decimal") {
    val bigItem = new java.math.BigDecimal("4242342434222232132.123123")
    val bytes = ByteBuffer.wrap(bigItem.unscaledValue().toByteArray)

    val decimalSchema = Schema.create(Schema.Type.BYTES)
    LogicalTypes.decimal(bigItem.precision(), bigItem.scale()).addToSchema(decimalSchema)

    val conversion = LogicalTypeConverters.toSqlValue(decimalSchema.getLogicalType, bytes, decimalSchema)

    assert(conversion.get.getClass == classOf[java.math.BigDecimal])
    assert(conversion.get == bigItem)
  }

  test("conversions from bytes type toSqlType of DecimalType") {
    val bigItem = new java.math.BigDecimal("4242342434222232132.123123")
    val bytes = ByteBuffer.wrap(bigItem.unscaledValue().toByteArray)

    val decimalSchema = Schema.create(Schema.Type.BYTES)
    LogicalTypes.decimal(bigItem.precision(), bigItem.scale()).addToSchema(decimalSchema)

    val conversion = LogicalTypeConverters.toSqlType(decimalSchema.getLogicalType)

    assert(conversion.get.getClass == classOf[DecimalType])
    assert(conversion.get.precision == bigItem.precision)
    assert(conversion.get.scale == bigItem.scale)
  }

  test("conversions from Spark SQL DecimalType to Avro Decimal type") {
    val schema = LogicalTypeConverters.convertDataTypeToLogical(DecimalType(5,2))

    assert(schema.getType == Schema.Type.BYTES)
    assert(schema.getLogicalType == LogicalTypes.decimal(5,2))
  }

  test("conversions from BigDecimal values to Avro Bytes") {
    val decimal = new BigDecimal("12312345322523535.123453")
    val expectedBytes = ByteBuffer.wrap(decimal.unscaledValue().toByteArray)

    val bytes = LogicalTypeConverters.convertToLogicalValue(decimal)

    assert(bytes == expectedBytes)
  }
}