package com.king.yl.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.king.yl.bean.AdsLog
import com.king.yl.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DateAreaCityAdsCountHandler {


  // 时间格式
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  // 定义RedisKey
  val redisKey = "day-area-city-ads"

  /** *
    * 保存数据到redis
    *
    * @param dateAreaCityAdsToCount
    */

  def saveDataToRedis(dateAreaCityAdsToCount: DStream[(String, Long)]) = {

    dateAreaCityAdsToCount.foreachRDD(rdd => {

      rdd.foreachPartition {
        items => {

          if (items.nonEmpty) {

            val jedisClient: Jedis = RedisUtil.getJedisClient

            val keyCountMap: Map[String, String] = items.map {
              case (key, count) => (key, count.toString)
            }.toMap

            //导入隐式转换
            import scala.collection.JavaConversions._

            //执行批量插入
            jedisClient.hmset(redisKey, keyCountMap)

            //关闭连接
            jedisClient.close()
          }

        }


      }


    })


  }


  /** *
    * 对于过滤后的数据进行按时间地区城市广告计数
    *
    * @param adsLogDStream
    * @return
    */
  def getDateAreaCityAdsCount(adsLogDStream: DStream[AdsLog]): DStream[(String, Long)] = {

    //    1.维度转换
    val getDateAreaCityAdsCount: DStream[(String, Long)] = adsLogDStream.map(adsLog => {

      //获取时间
      val dateStr: String = sdf.format(new Date(adsLog.timestamp))

      //拼接key
      val key = s"$dateStr:${adsLog.area}:${adsLog.city}:${adsLog.adid}"
      //返回
      (key, 1L)
    })

    //    2.有状态更新
    val dateAreaCityAdsToCount: DStream[(String, Long)] = getDateAreaCityAdsCount.updateStateByKey((seq: Seq[Long], state: Option[Long]) => {

      //当前批次求和
      val sum: Long = seq.sum

      //加入之前保存的状态
      val newState: Long = state.getOrElse(0L) + sum

      //返回
      Some(newState)
    })

    dateAreaCityAdsToCount
  }

}
