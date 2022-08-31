package com.aibee.dmp.tmm.util

import org.apache.spark.sql.SparkSession

object AddUDF {
  def addUDF(spark: SparkSession, clazz: String, jar_path: String, func_name: String): Unit = {
    spark.sql(s"""ADD JAR ${jar_path}""")
    spark.sql(s"""CREATE OR REPLACE TEMPORARY FUNCTION ${func_name}  AS '${clazz}'""")
    // spark.sql("show user functions").show(10,0)
  }
}
