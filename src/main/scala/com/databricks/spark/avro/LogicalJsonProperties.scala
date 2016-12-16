package com.databricks.spark.avro

import org.apache.avro.JsonProperties

/**
  * Created by adamf on 15/12/2016.
  */
class LogicalJsonProperties extends JsonProperties {
  override def getProp(name: String): Any = {
    val value = getJsonProp(name)
    if (value.getTextValue.isEmpty) null else value.getTextValue.toInt
  }
}
