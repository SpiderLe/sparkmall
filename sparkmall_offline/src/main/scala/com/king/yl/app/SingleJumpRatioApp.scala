package com.king.yl.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.king.yl.datamode.UserVisitAction
import com.king.yl.handler.SingleJumpHandler
import com.king.yl.util.JdbcUtil
import com.king.yl.utils.PropertiesUtil
import javafx.scene.input.DataFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SingleJumpRatioApp {

  def main(args: Array[String]): Unit = {

    //    1.创建session
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SingleJumpRatioApp")
      .enableHiveSupport()
      .getOrCreate()

    //    2.导入隐式转换
    import spark.implicits._

    //    3.获取配置文件
    //    读取配置文件
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val propertiesJson: String = properties.getProperty("condition.params.json")

    //    获取JSON对象
    val propertiesObj: JSONObject = JSON.parseObject(propertiesJson)
    //    获取跳转目标页面
    val targetPageFlow: String = propertiesObj.getString("targetPageFlow")

    val singlePageArray: Array[String] = targetPageFlow.split(",")

    //    去掉最后页面（1-6）
    val fromPageArray: Array[String] = singlePageArray.dropRight(1)
    //    去掉第一个页面（2-7）
    val toPageArray: Array[String] = singlePageArray.drop(1)
    //    获取跳转条件（1-2）（2-3）....
    val PageJumpArray: Array[String] = fromPageArray.zip(toPageArray).map {
      case (fromPage, toPage) => s"$fromPage-$toPage"
    }


    //    4.读取Hive数据,获取RDD： UserVisitActionRDD
    val userVisitActionRDD: RDD[UserVisitAction] = spark.sql("select * from user_visit_action").as[UserVisitAction].rdd

    //    5.过滤单页数据并计数
    val singlePageCount: RDD[(String, Long)] = SingleJumpHandler.getSinglePageCount(userVisitActionRDD, fromPageArray)
    singlePageCount.take(10).foreach(println)

    //    6.过滤跳转页面并计数
    val singleJumpCount: RDD[(String, Long)] = SingleJumpHandler.getSingleJumpCount(userVisitActionRDD, PageJumpArray)

    singleJumpCount.take(10).foreach(println)


    val taskId: String = UUID.randomUUID().toString

    //    7.将数据拉取到Driver端处理
    //    将singlePageCount设置为map: (5->1807)
    val singlePageCountMap: Map[String, Long] = singlePageCount.collect().toMap
    val singleJumpCountArray: Array[(String, Long)] = singleJumpCount.collect()

 


    val resultArray: Array[Array[Any]] = singleJumpCountArray.map {
      case (jumpPage, count) =>
        //        计算跳转率
        val ratio: Double = count.toDouble / singlePageCountMap.getOrElse(jumpPage.split("-")(0), 0L)

        Array(taskId, jumpPage, ratio)
    }

//    写入数据
    JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)", resultArray)

//    断开连接
    spark.close()


  }

}
