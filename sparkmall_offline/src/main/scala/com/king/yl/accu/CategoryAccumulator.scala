package com.king.yl.accu

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]]{

  private var categoryCount = new mutable.HashMap[String, Long]()

//  判空
  override def isZero: Boolean = categoryCount.isEmpty
//  拷贝
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryAccumulator
    accumulator.value ++= categoryCount
    accumulator
  }
//  重置
  override def reset(): Unit = {categoryCount.clear()}

// 区内聚合
  override def add(key: String): Unit = {
    categoryCount(key) = categoryCount.getOrElse(key, 0L) + 1L
  }

// 区间聚合
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    other.value.foreach(
      x => {categoryCount(x._1) = categoryCount.getOrElse(x._1,0L) + x._2}
    )


    //将其他的累计器中的值累加到当前累加器
   /* val stringToLong: mutable.HashMap[String, Long] = categoryCount.foldLeft(other.value) {
      case (otherMap, (category, count)) =>

      otherMap(category) = count + otherMap.getOrElse(category, 0L)

      otherMap
    }
    categoryCount = stringToLong*/


    /*  val value: mutable.HashMap[String, Long] = other.value

    value.foreach{
      case (category, count) => categoryCount(category) = value.getOrElse(category, 0L) + count
    }*/
  }



// 返回值
  override def value: mutable.HashMap[String, Long] = categoryCount
}
