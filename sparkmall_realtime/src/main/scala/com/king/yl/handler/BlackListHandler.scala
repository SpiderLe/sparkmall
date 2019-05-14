package com.king.yl.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.king.yl.bean.AdsLog
import com.king.yl.utils.RedisUtil
import org.apache.spark.{SparkContext, rdd}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {


  //  Redis中黑名单的key
  private val blackList = "blackList"

  // redis保存用户点击次数的key
  val redisKey = "date-user-ads"

  //  日期格式
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")


  /** *
    * 根据黑名单进行数据过滤
    *
    * @param sparkContext
    * @param adsLogDStream
    * @return
    */
  def filterDataByBlackList(sparkContext: SparkContext, adsLogDStream: DStream[AdsLog]): DStream[AdsLog] = {

    val filterAdsLogRDD: DStream[AdsLog] = adsLogDStream.transform(rdd => {

      //      1.获取redis客户端
      val client: Jedis = RedisUtil.getJedisClient

      //      2.取出黑名单
      val blackUserIdList: util.Set[String] = client.smembers(blackList)

      //      3.使用广播变量
      val blackListBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blackUserIdList)

      //      4.关闭连接
      client.close()

      val filterData: RDD[AdsLog] = rdd.mapPartitions(items => {

        items.filter(adsLog => {

          val userid: String = adsLog.userid
          !blackListBC.value.contains(userid)
        }
        )
      })
      filterData
    }
    )
    filterAdsLogRDD
  }


  /***
    * 检验数据，是否存在（每天每个用户对某个广告）点击数超过100的，则加入黑名单
    * @param adsLogDStream  封装之后的数据流
    */
  def checkUserToBlackList(adsLogDStream: DStream[AdsLog]): Unit = {

    //    1.转换维度
    val dateUserAdsOne: DStream[(String, Long)] = adsLogDStream.map(adsLog => {

      // 获取时间戳并将时间转换为年月日
      val date: String = sdf.format(new Date(adsLog.timestamp))

      (s"$date:${adsLog.userid}:${adsLog.adid}", 1L)

    })

    //    2.按照key聚合数据
    val dateUserAdsCount: DStream[(String, Long)] = dateUserAdsOne.reduceByKey(_ + _)

    //    3.存入redis
    dateUserAdsCount.foreachRDD(rdd => {

      // 对每个分区进行处理
      rdd.foreachPartition(items => {

        // 获取jedis
        val jedisClient: Jedis = RedisUtil.getJedisClient

        // 对每条数据进行处理
        items.foreach {
          case (key, count) => {

            // 写入Redis
            jedisClient.hincrBy(redisKey, key, count)

            // 读取数据count是否满足加入黑名单用户
            if (jedisClient.hget(redisKey, key).toLong > 60) {

              // 若超过读取userid,加入黑名单
              val userId: String = key.split(":")(1)

              jedisClient.sadd(blackList, userId)
            }
          }
        }

        // 关闭jedis连接
        jedisClient.close()

      })

    })


  }


}
