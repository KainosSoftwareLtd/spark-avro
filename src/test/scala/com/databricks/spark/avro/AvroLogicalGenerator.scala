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

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DatumWriter
import org.apache.avro.{Conversions, Schema}
import org.apache.commons.io.FileUtils

import scala.util.Random

/**
  * Created by shannonh on 13/12/2016.
  */
object AvroLogicalGenerator {
  val defaultNumberOfRecords = 10
  val defaultNumberOfFiles = 1
  val outputDir = "resources/decimals"
  val schemaPath = "src/test/resources/testdec.avsc"

  private[avro] def generateAvroFile(numberOfRecords: Int, fileIdx: Int) = {
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
    val decimal = schema.getField("decimals").schema()

    val rand = new Random()

    val conversion = new Conversions.DecimalConversion()

    for (idx <- 0 until numberOfRecords) {
      avroRec.put("decimals", conversion.toBytes(new java.math.BigDecimal("12313131313131.12312"),
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

    println(s"Decimals! Generating $numberOfFiles avro files with $numberOfRecords records each")

    FileUtils.deleteDirectory(new File(outputDir))
    new File(outputDir).mkdirs() // Create directory for output files

    for (fileIdx <- 0 until numberOfFiles) {
      generateAvroFile(numberOfRecords, fileIdx)
    }

    println("Generation finished")
  }
}
