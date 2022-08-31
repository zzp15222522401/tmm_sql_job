package com.aibee.dmp.tmm.common.util

import jodd.util.StringUtil

import java.sql.Connection
import scala.collection.mutable.ArrayBuffer

/**
 * 执行sql
 */
object ExecuteSql {
  def execute(conn:Connection, sql:String, args:String): Unit = {
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
          // 判断sql语句是哪一种，查询语句创建临时表，其他语句直接执行
          if (resultSql.toLowerCase().trim.startsWith("select")) {
            val selStatement = conn.prepareStatement(resultSql)
            selStatement.executeQuery()
          }
          else {
            val exStatement = conn.prepareStatement(resultSql)
            try {
              exStatement.executeUpdate()
            } catch {
              case e: Exception => e.printStackTrace()
            }
            finally {
              exStatement.close()
            }
          }
        }
      )
    }

  }
}
