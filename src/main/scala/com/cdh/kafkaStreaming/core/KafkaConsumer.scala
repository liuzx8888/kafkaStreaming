package com.cdh.kafkaStreaming.core
import java.sql.SQLException
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.cdh.kafkaStreaming.exception.KafkaStreamException
import com.cdh.kafkaStreaming.util.KakfaUtil
import java.util.logging.Logger

object KafkaConsumer {
    private val LOG = Logger.getLogger(KafkaConsumer.getClass.getName)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaConsumer")
    //刷新时间设置为1秒
    val streamingContext = new StreamingContext(conf, Seconds(5))
    try {
      val kafkaParams = KakfaUtil.apply.getKafkaProperties()
      //print("kafkaParams:"+kafkaParams  +kafkaParams.getClass)
      val topics = Array("THIS_REPL")
      val stream = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))
      stream.map(record => (record.key, record.value))

      //打印获取到的数据，因为1秒刷新一次，所以数据长度大于0时才打印
      stream.foreachRDD(f => {
        if (f.count > 0)
          f.foreach(f => println(f.value()))
      })
      streamingContext.start();
      streamingContext.awaitTermination();
    } catch {
      case t: KafkaStreamException => LOG.info(t.getMessage) // TODO: handle error
      case t: SQLException => LOG.info("请检查 连接数据的 [驱动\\用户名\\密码\\端口\\查询语句语法]是否正确！") // TODO: handle error
      case t: NullPointerException => LOG.info("SQL 语句返回值异常！") // TODO: handle error
    } 

  }
}