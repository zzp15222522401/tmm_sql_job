package com.aibee.dmp.tmm.util

import java.util.Properties

object PropUtil {

  def loadProperties(fileName:String):Properties = {
    println(s"properties--fileName===${fileName}")
    val properties = new Properties()
    val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName) //文件要放到resource文件夹下
    properties.load(inputStream)
    properties
  }

  def loadPropertiesWithEnv(envFlag:String):Properties = {
    val propFileName = s"${envFlag}.properties"

    loadProperties(propFileName)
  }
}
