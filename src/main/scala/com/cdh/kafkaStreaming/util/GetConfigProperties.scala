package com.cdh.kafkaStreaming.util

import scala.collection.immutable.Map
import java.util.Properties
import java.io.FileInputStream
import com.cdh.kafkaStreaming.exception.KafkaStreamException

class GetConfigProperties() {
  def loadProperties(propName: String): Properties = {
    if (propName.length() == 0) {
      throw new KafkaStreamException("�����ļ����Ʋ���Ϊ��!")
    } else {
      val properties = new Properties()
      val path = Thread.currentThread().getContextClassLoader.getResource(propName).getPath //�ļ�Ҫ�ŵ�resource�ļ�����
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
      throw new KafkaStreamException("�������ݱ�Ҫѡ���Ϊ��!")
    }
    val mapproperties = Map("mysqlconn" -> mysqlconn, "mysqluser" -> mysqluser, "mysqlpass" -> mysqlpass)
    return mapproperties
  }
}
// ��������
object GetConfigProperties {
  def apply = {
    new GetConfigProperties()
  }
}


