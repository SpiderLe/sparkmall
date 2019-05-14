package com.king.yl.app

import com.king.yl.datamode.ProductAndArea
import com.king.yl.handler.ProductAreaTop3Handler
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProductAndAreaTop3 {

  def main(args: Array[String]): Unit = {

    //    1.创建spark配置信息
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ProductAndAreaTop3")

    //    2.创建sparksession对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CategoryTop10")
      .config(sc).enableHiveSupport()
      .getOrCreate()

    //    3.读取数据返回 productAndAreaRDD
    //    ProductAndArea(2019-11-26,a5df0431-4002-4ff1-bf7b-2808080f43dd,37,商品_37,1,北京,华北)
    val productAndAreaRDD: RDD[ProductAndArea] = ProductAreaTop3Handler.readAndSelectData(spark)
    //    productAndAreaRDD.foreach(println)

    val productAndCityAreaCount: RDD[(String, Long)] = productAndAreaRDD.map(x => {
      (s"${x.area}-${x.product_name}-${x.city_name}", 1L)
    }).reduceByKey(_ + _)
    //(华中-商品_41-长沙,18)
    //(华东-商品_63-济南,23)
    //(华东-商品_89-上海,24)
    productAndCityAreaCount.take(10).foreach(println)


    val productAndAreaCount: RDD[(String, Long)] = productAndAreaRDD.map(x => {
      (s"${x.area}-${x.product_name}", 1L)
    }).reduceByKey(_ + _)
    //(东北_商品_4,52)
    //(西北_商品_47,40)
    //(东北_商品_48,81)
    //    productAndAreaCount.take(10).foreach(println)

    val areaAndProductCount: RDD[(String, (String, String, Long))] = productAndAreaCount.map {
      case (productAndArea, count) => {
        ((productAndArea.split("-")(0)), (productAndArea.split("-")(0), productAndArea.split("-")(1), count))
      }
    }
    //    (华北,(华北,商品_19,114))
    //    areaAndProductCount.take(5).foreach(println)

    val areaAndProductCountTop3: RDD[(String, String, Long)] = areaAndProductCount.groupByKey().flatMap {
      case (area, items) => {
        val areaAndProductCountSort: List[(String, String, Long)] = items.toList.sortWith {
          case (item1, item2) => {
            item1._3 > item2._3
          }
        }
        areaAndProductCountSort.take(3)
      }
    }
    //(西南,商品_39,99)
    //(西南,商品_63,88)
    //(西南,商品_9,86)
    //(西北,商品_90,59)
    //(西北,商品_50,58)
    //(西北,商品_12,57)
    areaAndProductCountTop3.foreach(println)



    val productAndCityAreaCountMap: Map[String, Long] = productAndCityAreaCount.collect().toMap

    val map: Map[String, Long] = productAndAreaCount.collect().toMap

    val AreaAndProductAndCityCount: RDD[((String, String), (String, Long))] = productAndCityAreaCount.map {
      case (area_product_city, count) => {
        val strArray: Array[String] = area_product_city.split("-")
        ((strArray(0), strArray(1)), (strArray(2),  count))
      }
    }
    //(华东,商品_89,上海,164,24)
    //(西南,商品_47,重庆,77,22)
    AreaAndProductAndCityCount.take(10).foreach(println)

    val AreaAndProductAndCityCountMap: Map[(String, String), (String, Long)] = AreaAndProductAndCityCount.collect().toMap

    val areaAndProductCountTop3ToCity: RDD[(String, String, Long, (String, Long))] = areaAndProductCountTop3.map {
      case (area, product, count) => {
        (area, product, count, AreaAndProductAndCityCountMap.getOrElse((area, product), ("UnKnow", 0L)))
      }
    }
    areaAndProductCountTop3ToCity.foreach(println)













    //    val areaToProductAndCityCount: RDD[((String, String), (String, String, String, Long, Long))] = AreaAndProductAndCityCount.map {
    //      case (area, product, city, areaCount, cityCount) => {
    //        ((area, product), (area, product, city, areaCount, cityCount))
    //      }
    //    }
    //    val areaCountTop3: RDD[(String, String, String, Long, Long)] = areaToProductAndCityCount.groupByKey().flatMap {
    //      case (area, items) => {
    //        val areaCountSort: List[(String, String, String, Long, Long)] = items.toList.sortWith {
    //          case (item1, item2) => {
    //            item1._4 > item2._4
    //          }
    //        }
    //        areaCountSort.take(3)
    //      }
    //    }
    //    areaCountTop3.foreach(println)


  }

}
