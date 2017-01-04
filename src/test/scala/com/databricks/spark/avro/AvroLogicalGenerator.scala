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

import java.io.File
import java.math.BigDecimal

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DatumWriter
import org.apache.avro.{Conversions, Schema}
import org.apache.commons.io.FileUtils

import scala.io.Source
import net.liftweb.json._

/**
  * Class to generate avro files with logical types
  * Currently only supports the avro schema in src/test/resources/logical.avsc
  * any json file to map into this schema will only be supported for a single decimal field
  * called "decimal" in the avro record
  */
object AvroLogicalGenerator {
  val defaultNumberOfRecords = 10
  val defaultNumberOfFiles = 1
  val outputDir = "src/test/resources/logical/"
  val schemaPath = "src/test/resources/logical.avsc"

  def createDecimalIterator = Iterator(new java.math.BigDecimal("12313131313131.12312"),
    new java.math.BigDecimal("43223334323.12423"),
    new java.math.BigDecimal("12.00000"),
    new java.math.BigDecimal("0.12312"),
    new java.math.BigDecimal("9088813123213332123222223334.12312"))

  private[avro] def generateAvroFile(numberOfRecords: Int, fileIdx: Int)
                                    (createDecimalIterator: () => Iterator[java.math.BigDecimal]) = {
    val schema = new Schema.Parser().parse(new File(schemaPath))
    val outputFile = new File(outputDir + "part" + fileIdx + ".avro")

    val genericData = new GenericData()
    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion())
    val datumWriter = genericData.createDatumWriter(schema)
    val dataFileWriter =
      new DataFileWriter[GenericData.Record](
        datumWriter.asInstanceOf[DatumWriter[GenericData.Record]])
    dataFileWriter.create(schema, outputFile)

    // Create data that we will put into the avro file
    val avroRec = new GenericData.Record(schema)
    val decimal = schema.getField("decimal").schema()

    val conversion = new Conversions.DecimalConversion()

    var decimals = createDecimalIterator()
    for (idx <- 0 until numberOfRecords) {
      if (!decimals.hasNext) {
        decimals = createDecimalIterator()
      }
      avroRec.put("decimal", conversion.toBytes(decimals.next(),
        decimal, decimal.getLogicalType))
      dataFileWriter.append(avroRec)
    }

    dataFileWriter.close()
  }

  def main(args: Array[String]) {

    var numberOfRecords = defaultNumberOfRecords
    var numberOfFiles = defaultNumberOfFiles

    if (args.size > 0) {
      numberOfRecords = args(0).toInt
    }

    if (args.size > 1) {
      numberOfFiles = args(1).toInt
    }

    println(s"Logical Types! Generating $numberOfFiles avro files with $numberOfRecords records each")

    FileUtils.deleteDirectory(new File(outputDir))
    new File(outputDir).mkdirs() // Create directory for output files

    val createIterator = if (args.size > 2) {
      println("Generating from Json file")
      () => iteratorFromJson(args(2))
    }
    else {
      println("Generating from code")
      () => createDecimalIterator
    }

    for (fileIdx <- 0 until numberOfFiles) {
      generateAvroFile(numberOfRecords, fileIdx)(createIterator)
    }

    println("Generation finished")
  }

  def iteratorFromJson(jsonPath: String): Iterator[java.math.BigDecimal] = {
    implicit val formats = DefaultFormats

    val source = new File(jsonPath)
    Source.fromFile(source).getLines().map {
      line =>
        val decimal = (parse(line) \ "decimal").extract[String]
        new BigDecimal(decimal)
    }
  }
}
