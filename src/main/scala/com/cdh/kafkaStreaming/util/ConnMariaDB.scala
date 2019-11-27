package com.cdh.kafkaStreaming.util

import java.sql.Connection
import java.sql.DriverManager
import com.cdh.kafkaStreaming.exception.KafkaStreamException
import java.util.logging.Logger

class ConnMariaDB() {
  private val LOG = Logger.getLogger(ConnMariaDB.getClass.getName)

  def connDB(): Connection = {
    val configProperties = GetConfigProperties.apply
    val connMariaDB = configProperties.loadProperties("config.properties")
    val MariaDbProperties = configProperties.MariaProperties(connMariaDB)

    val url = "jdbc:mariadb://" + MariaDbProperties.get("mysqlconn").head
    val driver = "org.mariadb.jdbc.Driver"
    val username = MariaDbProperties.get("mysqluser").head
    val password = MariaDbProperties.get("mysqlpass").head

    if (url.isEmpty() || username.isEmpty() || password.isEmpty()) {
      throw new KafkaStreamException("数据库连接选项, [url  username  pswword]  都不能为空!")
    }
    Class.forName(driver)
    val conn = DriverManager.getConnection(url, username, password)

    return conn
  }
}

// 伴生对象
object ConnMariaDB {
  def apply = {
    new ConnMariaDB()
  }
}