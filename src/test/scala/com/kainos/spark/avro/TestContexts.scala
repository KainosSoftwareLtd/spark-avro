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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Provides the spark SQL context to be shared by all test cases.
  */
object TestContexts {

  private val _sqlC: SQLContext = new SQLContext(
    new SparkContext(
    new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)))
  _sqlC.sparkContext.setLogLevel("OFF")

  def sqlContext: SQLContext = _sqlC
}
