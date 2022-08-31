package com.aibee.dmp.tmm.util

import com.aibee.dmp.tmm.common.util.SqlParsing
import jodd.util.StringUtil
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * @deprecated 数据源为hive的执行方式
 */
object HiveSource {
  def hiveSql(spark: SparkSession, sql: String, args: String) = {
    var resultSql = sql
    if (args.nonEmpty) {
      // 解析外部参数
      val argsList = args.split(";")
      val oldStrList = new ArrayBuffer[String]()
      val newStrList = new ArrayBuffer[String]()
      argsList.foreach(
        arg => {
          oldStrList += arg.split(":")(0)
          newStrList += arg.split(":")(1)
        }
      )
      println(oldStrList, newStrList)
      val sqlList = sql.split(";")
      sqlList.foreach(
        tmpSql => {
          // 替换多个外部参数
          resultSql = StringUtil.replace(SqlParsing.unzipStr(tmpSql), oldStrList.toArray, newStrList.toArray)
          println(s"替换参数后的sql:${resultSql}")
          spark.sql(resultSql)
        }
      )
    }
    else {
      val sqlList = sql.split(";")
      sqlList.foreach(
        tmpSql => {
          resultSql = SqlParsing.unzipStr(tmpSql)
          println(s"无替换参数sql:${resultSql}")
          spark.sql(resultSql)
        }
      )
    }
  }
}
