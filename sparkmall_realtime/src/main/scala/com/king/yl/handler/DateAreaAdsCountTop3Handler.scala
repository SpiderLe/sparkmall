package com.king.yl.handler


import com.king.yl.utils.RedisUtil
import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis


object DateAreaAdsCountTop3Handler {

  /***
    * 写入数据到Redis
    * @param dateAreaToAdsCountStr
    */
  def saveDataToRedis(dateAreaToAdsCountStr: DStream[(String, Map[String, String])]) = {

    dateAreaToAdsCountStr.foreachRDD{ rdd =>{

        rdd.foreachPartition{ items => {

          if(items.nonEmpty){

            // 获取Redis链接
            val client: Jedis = RedisUtil.getJedisClient

            // 遍历写入Redis
            items.foreach{ case (date, map)=>

              // 导入隐式转换
              import scala.collection.JavaConversions._

              // 写入Redis数据库
              client.hmset(s"top3_ads_per_day:$date", map)
            }

            // 关闭jedis
            client.close()
          }
        }}

      }
    }
  }


  /** *
    * 统计每天各个地区点击量前三的广告，并存入redis
    *
    * @param dateAreaCityAdsToCount
    * @return
    */

  def getDateAreaAdsCountTop3(dateAreaCityAdsToCount: DStream[(String, Long)]): DStream[(String, Map[String, String])] = {

    // 1.转换维度： RDD[(str, Long)] => RDD[(date, area, city, ads , count)]

    val dateAreaAdsToCount: DStream[((String, String, String), Long)] = dateAreaCityAdsToCount.map {
      case (dateAreaCityAds, count) =>
        //dateAreaCityAds : [s"$dateStr:${adsLog.area}:${adsLog.city}:${adsLog.adid}"]
        val splits: Array[String] = dateAreaCityAds.split(":")
        ((splits(0), splits(1), splits(3)), count)
    }.reduceByKey(_ + _)

    // 2.转换维度： RDD[(date, area, city, ads) , count] => RDD[(date, (area, (ads, count)))]

    val dateToAreaToAdsCount: DStream[(String, (String, (String, Long)))] = dateAreaAdsToCount.map {
      case ((date, area, ads), count) => (date, (area, (ads, count)))
    }

    // 3.按照date进行分组: RDD[(date, (area, (ads, count)))] => RDD[(date, Iter[(area, (ads, count)))]]

    val dateToAreaToAdsCountGroup: DStream[(String, Iterable[(String, (String, Long))])] = dateToAreaToAdsCount.groupByKey()

    //   3.1  在date分组内，对地区进行分组：RDD[(date, Iter[(area, (ads, count)))]] => RDD[(date, Iter[(area, Iter[(area,(ads, count))))]]]

    val dateAreaAdsCountTop3: DStream[(String, Map[String, List[(String, Long)]])] = dateToAreaToAdsCountGroup.mapValues(iter => {

      // 3.2 对地区进行分组
      val areaToAdsCountGroup: Map[String, Iterable[(String, (String, Long))]] = iter.groupBy(_._1)

      // 3.2 除去area: RDD[(date, Iter[(area, Iter[(area,(ads, count))))]]] => RDD[(date, Iter[(area, Iter[(ads, count))])]]

      // 错误代码，iter.groupBy(_._1)为scala方法，返回 Iterable[(String, (String, Long))]
      // val iterable: immutable.Iterable[Iterable[(String, (String, Long))]] = areaToAdsCountGroup.map{

      // case (area, items) => items}
      // (area, (area, (ads, count)))

      val areaToAdsAndCount: Map[String, Iterable[(String, Long)]] = areaToAdsCountGroup.map { case (area, items) =>

        //(area,(ads, count))
        (area, items.map(_._2))
      }

      // 3.4 对广告点击次数排序取前三
      val areaAdsCountTop3: Map[String, List[(String, Long)]] = areaToAdsAndCount.mapValues { items =>
        items.toList.sortWith { case (c1, c2) => c1._2 > c2._2 }
      }.take(3)

      areaAdsCountTop3
    })

    // 4.将时间地区组内的广告点击量集合转换为JSONStr
    val dateAreaToAdsCountStr: DStream[(String, Map[String, String])] = dateAreaAdsCountTop3.mapValues { areaAdsCountMap => {
      //隐式转换
      import org.json4s.JsonDSL._
      areaAdsCountMap.mapValues { list =>

        // 将list转换为json
        JsonMethods.compact(JsonMethods.render(list))

      }
    }
    }
    dateAreaToAdsCountStr
  }

  //

  // 4.存入redis: RDD[(date, Iter[(area, Iter[(ads, count)])] => RDD[(date, Iter[(area, json)])]

  //  4.1 Iter[(ads, count)])] => json




}
