package com.king.yl.app

import java.util.{Properties, UUID}

import com.king.yl.udf.CityRatioUDAF
import com.king.yl.utils.PropertiesUtil
import org.apache.spark.sql.{SaveMode, SparkSession}


object AreaProductTop3App {

  def main(args: Array[String]): Unit = {

    //    1.创建sparksession
    val spark: SparkSession = SparkSession.builder()
      .appName("AreaProductTop3App")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //    2.注册UDAF函数
    spark.udf.register("cityRatio", new CityRatioUDAF)


    //    3.读取Hive数据

    spark.sql("select area,ci.city_name,uv.click_product_id from user_visit_action uv join city_info ci on uv.city_id=ci.city_id where uv.click_product_id>0").createOrReplaceTempView("area_product_tmp")

    spark.sql("select area,click_product_id,count(*) area_product_count,cityRatio(city_name) city_ratio from area_product_tmp group by area,click_product_id").createOrReplaceTempView("area_product_count_tmp")

    spark.sql("select area,pi.product_name,area_product_count,city_ratio,rank() over(partition by area order by area_product_count desc) rk from area_product_count_tmp tmp join product_info pi on tmp.click_product_id=pi.product_id having rk<=3").createOrReplaceTempView("area_product_top3")

    //6.读取配置文件
    val properties: Properties = PropertiesUtil.load("config.properties")

    //获取taskID
    val taskId: String = UUID.randomUUID().toString

    //7.去除rk并写入MySQL
    spark.sql(s"select '$taskId' task_id,area,product_name,area_product_count product_count,city_ratio city_click_ratio from area_product_top3")
      .write
      .format("jdbc")
      .option("url", properties.getProperty("jdbc.url"))
      .option("user", properties.getProperty("jdbc.user"))
      .option("password", properties.getProperty("jdbc.password"))
      .option("dbtable", "area_count_info")
      .mode(SaveMode.Append)
      .save()

    //8.断开连接
    spark.close()


  }

}
