package com.aibee.dmp.tmm.sql_job

import com.aibee.dmp.tmm.common.util.{Config, ConnInfo, ExecuteSql, GetConn, SparkSessionFactory}
import com.aibee.dmp.tmm.util.{AddUDF, GetConnect, HiveSource, JdbcSource}

import java.sql.Connection

/**
 * @deprecated 主入口
 * @param argsInfo
 */

class SqlTask(argsInfo: ArgsInfo) {
  private val DATA_SOURCE_NAME:String = argsInfo.data_source_name
  private val JOB_ID:String = argsInfo.job_id
  private val ENV:String = argsInfo.env_flag

  def sqlTask(): Unit = {
    val spark = SparkSessionFactory.createSparkSession(this.getClass.getName)
    val conn = GetConnect.getConnection(ENV)
    val sql = s"select ${Config.SQL_JOB_INFO} from dmp.tmm_sql_job_info where sql_job_id ='${JOB_ID}' limit 1"
    println(sql)
    val preparedStatement = conn.prepareStatement(sql)
    val resultSet = preparedStatement.executeQuery(sql)
    while (resultSet.next()) {
        val source_type = resultSet.getString("source_type")
        val source_name = resultSet.getString("source_name")
        val udf_name = resultSet.getString("udf_name")
        val udf_jar =  resultSet.getString("udf_jar_name")
        val udf_clazz =  resultSet.getString("udf_jar_main_clazz")
        val main_sql = resultSet.getString("sql_body")
        val pre_sql = resultSet.getString("pre_sql")
        val after_sql = resultSet.getString("after_sql")
        val sql_args =  resultSet.getString("sql_args")
      // 添加自定义函数
      if (source_type.toLowerCase() == "hive" && udf_name.nonEmpty && udf_jar.nonEmpty && udf_clazz.nonEmpty){
        println(s"添加自定义函数${udf_name}")
        AddUDF.addUDF(spark, udf_clazz, udf_jar, udf_name)
      }
      //同一个数据源操作
      if (source_type.nonEmpty && source_name.nonEmpty){
        var sourceConnInfo:Map[String, Any] = null
        var source_conn:Connection = null
        if (source_type.toLowerCase() != "hive"){
          sourceConnInfo = ConnInfo.getSourceConnInfo(conn = conn, sourceName = source_name)
          source_conn = GetConn.getConnect(sourceConnInfo)
          if (pre_sql.nonEmpty) {
            ExecuteSql.execute(source_conn, pre_sql, sql_args)
          }
        }
        source_type.toLowerCase() match {
          case "hive" => HiveSource.hiveSql(spark, main_sql, sql_args)
          case _ => JdbcSource.jdbcSource(sourceConnInfo, main_sql, sql_args)
        }
        if (after_sql.nonEmpty) {
          ExecuteSql.execute(source_conn, after_sql, sql_args)
        }
        GetConnect.release(null, null, source_conn)
      }
      else{
        println("没有对应数据源信息，无法建立连接")
        return
      }
    }
    GetConnect.release(resultSet, preparedStatement, conn)
    spark.stop()
  }
}

object SqlTask {
  def apply(argsInfo: ArgsInfo): SqlTask = new SqlTask(argsInfo)

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      sys.error(s"args:${args}")
      return
    }
    val Array(data_source_name, job_id, env) = args
    val argsInfo = new ArgsInfo(data_source_name, job_id, env)
    SqlTask(argsInfo).sqlTask()
  }
}

