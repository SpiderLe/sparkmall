package com.king.yl.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.king.yl.bean.AdsLog
import com.king.yl.utils.RedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsClickCountHandler {

  private val sdf = new SimpleDateFormat("HH:mm")

  private val redisKey = "last_hour_ads_click"

  /***
    * 使用窗口函数进行最近一小时的数据统计，并将结果写入Redis
    *
    * @param filterAdsLogDStream
    */
  def saveLastHourClickCountToRedis(filterAdsLogDStream: DStream[AdsLog]) = {

    // 1.维度转换： AdsLog => ((adsId, hourMin), 1L)
    val adsAndHourToOne: DStream[((String, String), Long)] = filterAdsLogDStream.map(adsLog => {

      val hourMin: String = sdf.format(new Date(adsLog.timestamp))
      ((adsLog.adid, hourMin), 1L)
    })

    // 2.窗口内求和： ((adsId, hourMin), 1L) => ((adsId, hourMin), count)

    val adsAndHourToCount: DStream[((String, String), Long)] = adsAndHourToOne.reduceByKeyAndWindow((x: Long, y: Long) => x + y, Minutes(3), Minutes(1))


    // 3.按照adsId分组
    val adsToHourAndCount: DStream[(String, Iterable[(String, Long)])] = adsAndHourToCount.map { case ((adsId, hourMin), count) => {

      (adsId, (hourMin, count))

    }
    }.groupByKey()

    // 4.转换为json

    val adsToHourCountStr: DStream[(String, String)] = adsToHourAndCount.mapValues(list => {
      import org.json4s.JsonDSL._

      JsonMethods.compact(JsonMethods.render(list))
    })

    // 5.写入redis

    adsToHourCountStr.foreachRDD(rdd => {
      rdd.foreachPartition(items => {
        if (items.nonEmpty) {

          //获取连接
          val jedis: Jedis = RedisUtil.getJedisClient

          //导入隐式转换
          import scala.collection.JavaConversions._

          if (jedis.exists(redisKey)) {
            jedis.del(redisKey)
          }

          //批量插入数据
          jedis.hmset(redisKey, items.toMap)

          //关闭连接
          jedis.close()

        }
      })
    })



  }


}
