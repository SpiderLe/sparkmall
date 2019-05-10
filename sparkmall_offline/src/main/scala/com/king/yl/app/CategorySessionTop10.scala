package com.king.yl.app

import com.king.yl.datamode.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategorySessionTop10 {

  /** *
    *
    * @param taskId
    * @param sparkSession
    * @param userVisitActionRDD
    * @param categoryCountTop10
    * @return
    *
    * 1. 将categoryCountTop10作为广播变量
    * 2. 获取广播变量中的category_id
    * 3. 对数据集userVisitActionRDD进行过滤
    * 4. 计算category_session个数（category_session, count）
    * 5. 转换维度 （category, (category, session, count)）
    * 6. 按照category分组，排序 (category, session, count)
    * 7. 结构化数据，返回结果
    *
    */

  def getCategorySessionTop10(taskId: String, sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], categoryCountTop10: List[(String, mutable.HashMap[String, Long])]): RDD[Array[Any]] = {

    //    1. 将categoryCountTop10作为广播变量
    val categoryCountTop10BC: Broadcast[List[(String, mutable.HashMap[String, Long])]] = sparkSession.sparkContext.broadcast(categoryCountTop10)

    //    2. 获取广播变量中的 category_id
    val categoryCountTop10BCList: List[(String, mutable.HashMap[String, Long])] = categoryCountTop10BC.value
    println(categoryCountTop10BCList)
    val filterCategoryList: List[Long] = categoryCountTop10BCList.map(_._1.toLong)
    println(filterCategoryList)

    //    3. 对数据集 userVisitActionRDD 进行过滤
    val newUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      userVisitAction => {
        filterCategoryList.contains(userVisitAction.click_category_id)
      })

    println(newUserVisitActionRDD.foreach(println) + "!!!!!!!!!!!!!!!!!!!!")

    //    4. 计算category_session个数（category_session, count）
    val categorySessionCount: RDD[(String, Int)] = newUserVisitActionRDD.map(
      userVisitAction => {
        ((s"${userVisitAction.click_category_id}_${userVisitAction.session_id}"), 1)
      }).reduceByKey(_ + _)

    //    5. 转换维度
    val categoryToSessionCount: RDD[(String, (String, String, Int))] = categorySessionCount.map(
      x => {
        val categorySession: Array[String] = x._1.split("_")
        (categorySession(0), (categorySession(0), categorySession(1), x._2))
      }
    )

    //    6. 按照category分组，排序
    val categorySessionTop10: RDD[(String, String, Int)] = categoryToSessionCount.groupByKey().flatMap {
      case (categoryId, items) => {
        val categorySessionSort: List[(String, String, Int)] = items.toList.sortWith {
          case (item1, item2) => item1._3 > item2._3
        }
        categorySessionSort.take(10)
      }
    }

    //    7. 结构化数据，返回结果
    val resultArray: RDD[Array[Any]] = categorySessionTop10.map(
      x => {
        Array(taskId, x._1, x._2, x._3)
      }
    )
    resultArray
  }


}
