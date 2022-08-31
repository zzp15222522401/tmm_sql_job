package com.aibee.dmp.tmm.sql_job

case class ArgsInfo(data_source_name:String, job_id:String, env_flag:String) {
  override def toString: String =
    s"""
       |data_source_name: ${data_source_name}
       |job_id: ${job_id}
       |env_flag: ${env_flag}
       |""".stripMargin
}
