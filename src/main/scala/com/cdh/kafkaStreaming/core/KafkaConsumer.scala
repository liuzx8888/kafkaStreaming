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
    //ˢ��ʱ������Ϊ1��
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

      //��ӡ��ȡ�������ݣ���Ϊ1��ˢ��һ�Σ��������ݳ��ȴ���0ʱ�Ŵ�ӡ
      stream.foreachRDD(f => {
        if (f.count > 0)
          f.foreach(f => println(f.value()))
      })
      streamingContext.start();
      streamingContext.awaitTermination();
    } catch {
      case t: KafkaStreamException => LOG.info(t.getMessage) // TODO: handle error
      case t: SQLException => LOG.info("���� �������ݵ� [����\\�û���\\����\\�˿�\\��ѯ����﷨]�Ƿ���ȷ��") // TODO: handle error
      case t: NullPointerException => LOG.info("SQL ��䷵��ֵ�쳣��") // TODO: handle error
    } 

  }
}