package com.aibee.dmp.tmm.util

import com.aibee.dmp.tmm.common.util.{GetConn, SqlParsing}
import jodd.util.StringUtil

import java.sql.PreparedStatement
import scala.collection.mutable.ArrayBuffer

/**
 * @deprecated 数据源为jdbc方式的执行sql
 */
object JdbcSource {
  def jdbcSource(sourceConfig: Map[String, Any], sql: String, args: String): Unit = {

    val argsList = args.split(";")

    val oldStrList = new ArrayBuffer[String]()
    val newStrList = new ArrayBuffer[String]()
    argsList.foreach(
      tmpArg => {
        oldStrList += tmpArg.split(":")(0)
        newStrList += tmpArg.split(":")(1)
      }
    )
    val conn = GetConn.getConnect(sourceConfig)
    val sqlList = sql.split(";")
    var statement:PreparedStatement = null
    sqlList.foreach(
      tmpSql => {
        val resultSql = StringUtil.replace(SqlParsing.unzipStr(tmpSql), oldStrList.toArray, newStrList.toArray)
        try {
          statement = conn.prepareStatement(resultSql)
          if (resultSql.toLowerCase().trim.startsWith("select")) {
            statement.executeQuery()
          }
          else {
            statement.executeUpdate()
          }
        }catch {
          case e:Exception => e.printStackTrace()
        }
        finally {
          statement.close()
          conn.close()
        }
      }
    )
  }
}
