package com.aibee.dmp.tmm.common.util

import java.sql.{Connection, PreparedStatement}


/**
 * 根据数据源名称获取数据源连接信息
 */

object ConnInfo {
  def getSourceConnInfo(conn:Connection, sourceName:String): Map[String, Any] = {
    val sql = "select data_source_type,data_source_name,driver,host,port,username,password,url from " +
      s"dmp.dis_platform_connect_info where data_source_name='${sourceName}'"
    println(sql)
    var connInfo:Map[String, Any] = null
    var statement:PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      val rs = statement.executeQuery()
      while (rs.next()) {
        val data_source_type = rs.getString("data_source_type")
        val driver = rs.getString("driver")
        val host = rs.getString("host")
        val port = rs.getInt("port")
        val username = rs.getString("username")
        val password = EncryptUtil.decrypt(Config.KEY, rs.getString("password"))
        val url = rs.getString("url")
        connInfo = Map(
          "source_type" -> data_source_type,
          "driver" -> driver,
          "host" -> host,
          "port" -> port,
          "username" -> username,
          "password" -> password,
          "url" -> url
        )
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      statement.close()
    }
    connInfo
  }
}
