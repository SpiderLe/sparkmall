package com.king.yl.app

import com.king.yl.bean.AdsLog
import com.king.yl.handler.{BlackListHandler, DateAreaAdsCountTop3Handler, DateAreaCityAdsCountHandler, LastHourAdsClickCountHandler}
import com.king.yl.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {

  def main(args: Array[String]): Unit = {

    //    1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp").setMaster("local[*]")

    //    2.创建sparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // ***Checkpoint***
    ssc.sparkContext.setCheckpointDir("./ck1")

    //    3.制定消费的主题
    val topic = "ads_log"

    //    4.读取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaDStream(ssc, Array(topic))


    //    5.将ConsumerRecord转换为javaBean
    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map {
      items => {
        val itemList: Array[String] = items.value().split(" ")

        //封装为样例类对象
        AdsLog(itemList(0).toLong, itemList(1), itemList(2), itemList(3), itemList(4))
      }
    }

//    adsLogDStream.print()

    //    6.查询黑名单进行数据过滤

    val filterAdsLogDStream: DStream[AdsLog] =
      BlackListHandler.filterDataByBlackList(ssc.sparkContext, adsLogDStream)

    filterAdsLogDStream.print()

    // 需求六：实时统计每天各地区各城市各广告的点击流量，并将其存入Redis

    // 统计每天每个地区每个城市每个广告的点击次数
    val dateAreaCityAdsToCount: DStream[(String, Long)] = DateAreaCityAdsCountHandler.getDateAreaCityAdsCount(filterAdsLogDStream)

    // 将每天每个地区每个城市每个广告的点击次数写入Redis
    DateAreaCityAdsCountHandler.saveDataToRedis(dateAreaCityAdsToCount)

    // 需求七：每天各地区热门广告点击量：每天各地区 top3 热门广告
    val dateAreaToAdsCountStr: DStream[(String, Map[String, String])] = DateAreaAdsCountTop3Handler.getDateAreaAdsCountTop3(dateAreaCityAdsToCount)
    // 写入数据到redis
    DateAreaAdsCountTop3Handler.saveDataToRedis(dateAreaToAdsCountStr)



    // 需求8： 最近一小时广告点击量
    // 使用窗口函数进行最近一小时的数据统计，并将结果写入Redis。
    LastHourAdsClickCountHandler.saveLastHourClickCountToRedis(filterAdsLogDStream)





    // 7.校验数据是否需要加入黑名单，如果超过100次，则加入黑名单

    BlackListHandler.checkUserToBlackList(filterAdsLogDStream)

    //    8.启动
    ssc.start()
    ssc.awaitTermination()


  }

}
