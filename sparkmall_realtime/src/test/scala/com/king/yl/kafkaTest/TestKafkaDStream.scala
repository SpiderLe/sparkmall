package com.king.yl.kafkaTest

import java.util.Properties

import com.king.yl.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestKafkaDStream {

  def main(args: Array[String]): Unit = {

//    1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setAppName("TestKafkaDStream").setMaster("local[*]")

//    2.创建sparkstreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

//    3.获取topic信息
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")

//    4.获取kafka流
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc,Array(topic))

    kafkaDstream.map{
      records => {
        val log: String = records.value()
        log
      }
    }.print()

    ssc.start()
    ssc.awaitTermination()



  }

}
