package com.king.yl.handler

import com.alibaba.fastjson.JSONObject
import com.king.yl.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object CategoryTop10Handler {

  /** *
    * 读取Hive中的数据并按条件进行过滤
    *
    * @param conditionObj 过滤条件
    * @param spark        sparkSession
    * @return
    */
  def readAndFilterData(conditionObj: JSONObject, spark: SparkSession): RDD[UserVisitAction] = {

    //    1.导入隐式转换
    import spark.implicits._

    //    2.获取过滤参数
    val startDate: String = conditionObj.getString("startDate")
    val endDate: String = conditionObj.getString("endDate")
    val startAge: String = conditionObj.getString("startAge")
    val endAge: String = conditionObj.getString("endAge")

    //    3.构建Sql语句
    val sql = new StringBuilder("select a.* from user_visit_action a join user_info b on a.user_id=b.user_id where 1=1")

    println(sql)
    //    4.判断过滤参数并追加条件
    if (startDate != null) {
      sql.append(s" and date>='$startDate'")
    }
    if (endDate != null) {
      sql.append(s" and date <= '$endDate'")
    }
    if (startAge != null) {
      sql.append(s" and age>=$startAge")
    }
    if (endAge != null) {
      sql.append(s" and age<=$endAge")
    }

//    5.执行查询
    val dataFrame: DataFrame = spark.sql(sql.toString())

//    6.将dataFrame转换为RDD   (先将dataFrame转换为Dataset,带类型，再由DataSet转换为RDD)
    val userVisitActionRDD: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd

//    7.返回RDD
    userVisitActionRDD
  }
}
