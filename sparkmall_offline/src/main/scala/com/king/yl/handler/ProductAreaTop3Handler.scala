package com.king.yl.handler

import com.king.yl.datamode.ProductAndArea
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/** *
  * select 	a.date, a.session_id, a.click_product_id, c.product_name, a.city_id, b.city_name, b.area
  * from user_visit_action a
  * join  city_info b
  * on a.city_id=b.city_id
  * join  product_info c
  * on a.click_product_id=c.product_id
  */
object ProductAreaTop3Handler {


  def readAndSelectData(spark: SparkSession): RDD[ProductAndArea] = {
    //    1.导入隐式转换
    import spark.implicits._

    //    2.构建Sql语句
    val sql = new StringBuilder()
    sql.append("select a.date, a.session_id, a.click_product_id, c.product_name, a.city_id, b.city_name, b.area ")
    sql.append("from user_visit_action a join city_info b on a.city_id=b.city_id join product_info c on a.click_product_id=c.product_id")

    println(sql)
    val dataFrame: DataFrame = spark.sql(sql.toString())
    val productAndArea: RDD[ProductAndArea] = dataFrame.as[ProductAndArea].rdd

    //      productAndArea.foreach(println)

    productAndArea


  }

}
