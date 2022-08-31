package com.aibee.dmp.tmm.common.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

/**
 * @deprecated 数据源连接方式
 */
object GetConn {
  private var connection: Connection = _

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


  def getConnect(driver:String, url:String, user:String, pass:String): Connection = {
    Class.forName(driver)
    try {
      connection = DriverManager.getConnection(url, user, pass)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    connection
  }

  def getConnect(connInfo: Map[String,Any]): Connection = {
    val driver = connInfo("driver").toString
    val url = connInfo("url").toString
    val user = connInfo("username").toString
    val pass = connInfo("password").toString
    Class.forName(driver)
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
