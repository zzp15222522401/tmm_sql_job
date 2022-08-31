package com.aibee.dmp.tmm.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

/**
 * @deprecated 数据源连接方式
 */
object GetConnect {

  private var connection: Connection = _

  def getConnection(envFlag:String): Connection = {
    val DBProp = PropUtil.loadPropertiesWithEnv(envFlag)
    Class.forName(DBProp.getProperty("jdbc.driverClass"))
    val url = s"jdbc:mysql://${DBProp.getProperty("jdbc.host")}:${DBProp.getProperty("jdbc.port")}/${DBProp.getProperty("jdbc.dbName")}?useSSL=false"
    try {
      connection = DriverManager.getConnection(url, DBProp.getProperty("jdbc.username"), DBProp.getProperty("jdbc.password"))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    connection
  }

  def getConnect(clazz:String, host:String, port:Int, dbName:String, user:String, pass:String): Connection = {
    Class.forName(clazz)
    val url = s"jdbc:mysql://${host}:${port}/${dbName}?useSSL=false"
    try {
      connection = DriverManager.getConnection(url, user, pass)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    connection
  }

  def release(rs:ResultSet, statement:Statement, conn:Connection) {
    if(rs != null) {
      try {
        rs.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    //关闭Statement对象
    if(statement != null) {
      try {
        statement.close();
      } catch  {
        case e: Exception => e.printStackTrace()
      }
    }

    //关闭conn对象
    if(conn != null) {
      try {
        conn.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
