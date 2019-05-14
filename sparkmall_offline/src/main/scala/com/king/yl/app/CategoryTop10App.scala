package com.king.yl.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.king.yl.accu.CategoryAccumulator
import com.king.yl.datamode.UserVisitAction
import com.king.yl.handler.CategoryTop10Handler
import com.king.yl.util.JdbcUtil
import com.king.yl.utils.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


object CategoryTop10App {

  def main(args: Array[String]): Unit = {

    //    1.创建spark配置信息
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10")

    //    2.创建sparksession对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CategoryTop10")
      .config(sc).enableHiveSupport()
      .getOrCreate()

    //    3.获取文件中的过滤条件
    val conditionPro: Properties = PropertiesUtil.load("conditions.properties")
    val conditionJSON: String = conditionPro.getProperty("condition.params.json")

    //    4.将json转换为对象
    val conditionObj: JSONObject = JSON.parseObject(conditionJSON)

    //    5.读取Hive中的数据并按条件进行过滤,传入过滤条件的json对象和sparkSession
    val userVisitActionRDD: RDD[UserVisitAction] = CategoryTop10Handler.readAndFilterData(conditionObj, spark)

    //    测试读取RDD10条数据
//    userVisitActionRDD.take(10).foreach(println)

    //    6.创建累加器并注册
    val accu = new CategoryAccumulator
    spark.sparkContext.register(accu, "categoryCountTop10")

    //    7.使用累加器计算三种类型数据, (点击, 下单, 支付)

    userVisitActionRDD.foreach(userVisitAction => {
      //        判断是否是点击数据
      if (userVisitAction.click_category_id != -1) {
        //          是点击数据,click_category_id加1
        accu.add(s"click_${userVisitAction.click_category_id}")
      }
      //        判断是否是订单数据
      else if (userVisitAction.order_category_ids != null) {
        //          是订单数据,切分订单order_category_ids
        userVisitAction.order_category_ids.split(",").foreach(
          order_id => accu.add(s"order_${order_id}")
        )
      }
      //        判断是否是支付数据
      else if (userVisitAction.pay_category_ids != null) {
        //          是支付数据,切分pay_category_ids
        userVisitAction.pay_category_ids.foreach(
          pay_id => accu.add(s"pay_$pay_id")
        )
      }
    })

    //     8.获取累加器的值 (order_1, 100)
    val categoryCountMap: mutable.HashMap[String, Long] = accu.value
    //        categoryCountMap.foreach(println)

    //   9.按照品类id进行分组 (1, (click_1,100),(order_1,90),(pay_1,9))
    //    (8,Map(click_8 -> 940, pay_8 -> 464, order_8 -> 322))
    val categoryCountMapGroup: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy(_._1.split("_")(1))
//    categoryCountMapGroup.take(10).foreach(println)

    //    10.排序
    val categoryCountTop10: List[(String, mutable.HashMap[String, Long])] = categoryCountMapGroup.toList.sortWith({
      case (c1, c2) => {
        val category1: String = c1._1
        val category2: String = c2._1
        val categoryCount1: mutable.HashMap[String, Long] = c1._2
        val categoryCount2: mutable.HashMap[String, Long] = c2._2

        //        先比较点击次数
        if (categoryCount1.getOrElse(s"click_$category1", 0L) > categoryCount2.getOrElse(s"click_$category2", 0L)) {
          true
        } else if (categoryCount1.getOrElse(s"click_$category1", 0L) == categoryCount2.getOrElse(s"click_$category2", 0L)) {
          //          如果点击次数相同，则比较下单次数
          if (categoryCount1.getOrElse(s"order_$category1", 0L) > categoryCount2.getOrElse(s"order_$category2", 0L)) {
            //          点击次数相同，比较下单次数
            true
          } else if (categoryCount1.getOrElse(s"order_$category1", 0L) == categoryCount2.getOrElse(s"order_$category2", 0L)) {
            //          订单次数也相同，比较支付数量
            categoryCount1.getOrElse(s"pay_$category1", 0L) > categoryCount2.getOrElse(s"pay_$category2", 0L)
          } else {
            //              点击次数相同，下单次数少
            false
          }
        } else {
          //            点击次数少
          false
        }
      }
    }
    ).take(10)

        categoryCountTop10.foreach(println)

    //    11.   将result进行结构调整，每一行成为一个Array
    //    	resultArray =(taskId,1984,609,50)
    val taskId: String = UUID.randomUUID().toString

    val resultArray: List[Array[Any]] = categoryCountTop10.map {
      case (category, categoryCount) =>
        Array(taskId,
          category,
          categoryCount.getOrElse(s"click_$category", 0L),
          categoryCount.getOrElse(s"order_$category", 0L),
          categoryCount.getOrElse(s"pay_$category", 0L))
    }

    //    12.数据插入到Mysql
//        JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", resultArray)

//    （*************************************************************************************************)
    //    13.计算前10品类的前十点击session
    val categorySessionTop10: RDD[Array[Any]] = CategorySessionTop10.getCategorySessionTop10(taskId, spark, userVisitActionRDD, categoryCountTop10)
    println(categorySessionTop10.foreach(println) + "*************************")
    val categorySessionTop10Array: Array[Array[Any]] = categorySessionTop10.collect()
    //    println(categorySessionTop10Array)

    //    14.数据插入到Mysql
    JdbcUtil.executeBatchUpdate("insert into category_session_top10 values(?,?,?,?)", categorySessionTop10Array)


    println("***********程序执行完毕！***********")

    //    13.关闭连接
    spark.close()

  }


}
