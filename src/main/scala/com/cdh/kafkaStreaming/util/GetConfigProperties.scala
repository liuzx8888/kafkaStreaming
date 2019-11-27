package com.cdh.kafkaStreaming.util

import scala.collection.immutable.Map
import java.util.Properties
import java.io.FileInputStream
import com.cdh.kafkaStreaming.exception.KafkaStreamException

class GetConfigProperties() {
  def loadProperties(propName: String): Properties = {
    if (propName.length() == 0) {
      throw new KafkaStreamException("属性文件名称不能为空!")
    } else {
      val properties = new Properties()
      val path = Thread.currentThread().getContextClassLoader.getResource(propName).getPath //文件要放到resource文件夹下
      properties.load(new FileInputStream(path))
      return properties
    }
    return null
  }

  def MariaProperties(properties: Properties): Map[String, String] = {
    val mysqlconn = properties.getProperty("MYSQL_CONNECT")
    val mysqluser = properties.getProperty("MYSQL_USER")
    val mysqlpass = properties.getProperty("MYSQL_PASS")

    if (mysqlconn.isEmpty() || mysqluser.isEmpty() || mysqlpass.isEmpty()) {
      throw new KafkaStreamException("配置内容必要选项不能为空!")
    }
    val mapproperties = Map("mysqlconn" -> mysqlconn, "mysqluser" -> mysqluser, "mysqlpass" -> mysqlpass)
    return mapproperties
  }
}
// 伴生对象
object GetConfigProperties {
  def apply = {
    new GetConfigProperties()
  }
}


