package com.king.yl.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


/** *
  * 使用spark读取kafka中的数据
  */
object MyKafkaUtil {

  //  读取配置文件中的kafka参数
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val brokers: String = properties.getProperty("kafka.broker.list")
  private val group: String = properties.getProperty("kafka.group")

  val kafkaParams = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.GROUP_ID_CONFIG -> group,
    //    K,V序列化
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    //    自动offset重置
    //    满足条件后按照最大的组进行消费
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"

  )

  //创建kafka流
  def getKafkaDStream(ssc: StreamingContext, topics: Array[String]): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    kafkaDStream

  }

}
