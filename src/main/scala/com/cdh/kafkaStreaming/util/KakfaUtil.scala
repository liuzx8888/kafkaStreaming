package com.cdh.kafkaStreaming.util
import scala.collection.mutable.Map
import com.cdh.kafkaStreaming.exception.KafkaStreamException
import scala.collection.mutable.LinkedHashMap
import java.util.logging.Logger


class KakfaUtil {
  private val LOG = Logger.getLogger(KakfaUtil.getClass.getName)
  def getKafkaProperties(): Map[String, Object] = {
    val kafkaConfig: Map[String, Object] = new LinkedHashMap
    val connMariaDB = ConnMariaDB.apply
    val connDb = connMariaDB.connDB()
    val rs = connDb.createStatement().executeQuery("select name,value from kafka_config")
    while (rs.next()) {
      kafkaConfig.put(rs.getString(1), rs.getString(2))
    }

    if (kafkaConfig.isEmpty)
      throw new KafkaStreamException("请检查 kafka_config表是否设置好相关参数！")
    return kafkaConfig
  }

}

// 伴生对象
object KakfaUtil {
  def apply = {
    new KakfaUtil()
  }
}