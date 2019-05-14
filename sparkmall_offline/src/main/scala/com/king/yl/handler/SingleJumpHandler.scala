package com.king.yl.handler

import com.king.yl.datamode.UserVisitAction
import org.apache.spark.rdd.RDD

object SingleJumpHandler {

  /** *
    * 获取单跳的点击次数
    *
    * @param userVisitActionRDD
    * @param PageJumpArray
    */
  def getSingleJumpCount(userVisitActionRDD: RDD[UserVisitAction], PageJumpArray: Array[String]) = {

    //   1.获取数据集
    val sessionToTimeAndPage: RDD[(String, (String, Long))] = userVisitActionRDD.map {
      case userVisitAction => (userVisitAction.session_id, (userVisitAction.action_time, userVisitAction.page_id))
    }
    //    2.根据session进行排序
    val singleJumpPageAndOne: RDD[(String, Long)] = sessionToTimeAndPage.groupByKey().flatMap {

      case (session, items) => {
        val sortedList: List[(String, Long)] = items.toList.sortBy(_._1)
        //        获取排序后的pages
        val pages: List[Long] = sortedList.map(_._2)

        val fromPage: List[Long] = pages.dropRight(1)
        val toPage: List[Long] = pages.drop(1)
        val singleJumpPages: List[String] = fromPage.zip(toPage).map {
          case (from, to) => s"$from-$to"
        }

        //        按照条件进行跳转
        val filterJumpPages: List[String] = singleJumpPages.filter(PageJumpArray.contains)
        //        返回类型 ：(1-2, 1)
        filterJumpPages.map((_, 1L))
      }
    }
    //    3.汇总求和
    val singleJumpPageAndCount: RDD[(String, Long)] = singleJumpPageAndOne.reduceByKey(_ + _)

    //    4.返回
    singleJumpPageAndCount


  }


  /** *
    * 获取单个页面的点击次数
    *
    * @param userVisitActionRDD
    * @param fromPageArray
    */
  def getSinglePageCount(userVisitActionRDD: RDD[UserVisitAction], fromPageArray: Array[String]): RDD[(String, Long)] = {


    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      userVisitAction => {
        fromPageArray.contains(userVisitAction.page_id.toString)
      }
    )
    val singlePageCount: RDD[(String, Long)] = filterUserVisitActionRDD.map(
      userVisitAction => {
        (userVisitAction.page_id.toString, 1L)
      }
    ).reduceByKey(_ + _)
    singlePageCount
  }


}
