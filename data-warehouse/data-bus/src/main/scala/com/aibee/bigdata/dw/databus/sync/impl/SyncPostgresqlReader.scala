package com.aibee.bigdata.dw.databus.sync.impl
import java.util.Properties

import com.aibee.bigdata.dw.databus.Properties
import com.aibee.bigdata.dw.databus.config.DataBusConfig
import com.aibee.bigdata.dw.databus.sync.{AdHocReader, SyncMeta}
import com.aibee.bigdata.dw.databus.utils.PostgresqlUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class SyncPostgresqlReader(override val spark: SparkSession, propFile: String) extends AdHocReader {
  override def read(sql: String, srcDb: String, srcTbl: String): DataFrame = {
    val properties = fromPropFile()

    val connId = properties.getProperty(s"${SyncMeta.READER}.${SyncMeta.CONN_ID}")

    val db: String = getValEntity(properties, srcDb, s"${SyncMeta.READER}.${SyncMeta.DB}")
    val tbl: String = getValEntity(properties, srcTbl, s"${SyncMeta.READER}.${SyncMeta.TABLE}")

    val isPartition = properties.getProperty(s"${SyncMeta.READER}.${SyncMeta.IS_PARTITION}", "false").toLowerCase()

    val meta = getSyncJdbcMeta(connId)

    val url = meta.getOrElse("url", "")
    val user = meta.getOrElse("userName", "")
    val pwd = meta.getOrElse("password", "")


    val sqlStatement = sql.replace("{db}", db).replace("{table}", tbl)

    val df = if(isPartition.equals("true")){
      val lBound = properties.getProperty(s"${SyncMeta.READER}.${SyncMeta.PARTITION_LOWER_BOUND}")
      val uBound = properties.getProperty(s"${SyncMeta.READER}.${SyncMeta.PARTITION_UPPER_BOUND}")
      val partitions = properties.getProperty(s"${SyncMeta.READER}.${SyncMeta.PARTITION_NUMS}")
      val partitionCol = properties.getProperty(s"${SyncMeta.READER}.${SyncMeta.PARTITION_COLUMN}")

      //      val partitionArr = MysqlUtil.getPartitionArray(tableCnt.toInt, pageSize.toInt, partitionCol, tbl)

      PostgresqlUtil.readPartitionedPostgresql(spark, url, user, pwd, sqlStatement,
        isPartition, partitionCol, lBound, uBound, partitions)
    }else{
      println("not partition --> ")
      PostgresqlUtil.readPostgresqlTbl(spark, url, user, pwd, tbl, sqlStatement)
    }
    df
  }

  //  def readMysql(url: String, userName: String, password: String, mtbl: String,
  //                partitionCol: String, partitionNums: String, lBound: String, uBound: String) = {
  //    if(StringUtils.isBlank(partitionCol)){
  //      MysqlUtil.readMysqlTbl(spark, url, userName, password, mtbl)
  //    }else{
  //      MysqlUtil.readMysqlTbl(spark, url, userName, password, mtbl, partitionCol, partitionNums.toInt, lBound.toInt, uBound.toInt)
  //    }
  //  }

  override val PROP_FILE: String = propFile
}

