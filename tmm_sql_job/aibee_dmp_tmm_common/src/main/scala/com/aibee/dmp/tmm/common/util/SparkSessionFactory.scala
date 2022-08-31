package com.aibee.dmp.tmm.common.util

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def createSparkSession(appName:String, master:String="", parallels:Int=3): SparkSession = {
    val spark: SparkSession =
      if("local".equals(master) || "local" ==System.getProperty("spark.master")){
        SparkSession.builder()
          .master("local[*]")
          .appName(appName)
          .config("hive.exec.dynamic.partition",true)
          .config("hive.exec.dynamic.partition.mode","nonstrict")
          .config("spark.sql.broadcastTimeout", "36000")
          .config("spark.sql.shuffle.partitions",3)//sparkSql的默认shuffle 分区数
          .config("spark.default.parallelism",3)//rdd 默认并行度
          .enableHiveSupport()
          .getOrCreate()
      }else{
        SparkSession.builder()
          .appName(appName)
          .config("hive.exec.dynamic.partition",true)
          .config("hive.exec.dynamic.partition.mode","nonstrict")
          .config("spark.sql.broadcastTimeout", "36000")  // 广播等待超时时间, 默认300s
          .config("spark.sql.shuffle.partitions",parallels)//sparkSql的默认shuffle 分区数
          .config("spark.default.parallelism",parallels)//rdd 默认并行度
          .enableHiveSupport()
          .getOrCreate()
      }
    spark
  }
}
