package com.king.yl.udf


import com.king.yl.bean.CityRatio
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}


class CityRatioUDAF extends UserDefinedAggregateFunction {

  //  输入数据类型
  override def inputSchema: StructType = StructType(StructField("city_name", StringType) :: Nil)

  //  缓存数据类型
  override def bufferSchema: StructType = StructType(StructField("city_count", MapType(StringType, LongType))
    :: StructField("total_count", LongType) :: Nil)

  //  输出数据类型
  override def dataType: DataType = StringType

  //  函数稳定性
  override def deterministic: Boolean = true

  //  对缓存进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer(0) = Map[String, Long]()
    buffer(1) = 0L

  }

  //  区内合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //    获取第一个字段：city_count: [(北京->3),(西安->45)...]
    val city_count: Map[String, Long] = buffer.getAs[Map[String, Long]](0)

    //    取出map中传入的城市对应value
    val count: Long = city_count.getOrElse(input.getString(0), 0L)
    buffer(0) = city_count + (input.getString(0) -> (count + 1L))

    //    记录总数
    buffer(1) = buffer.getLong(1) + 1L
  }

  //  区间合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val city_count1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val city_count2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)

    val total_count1: Long = buffer1.getLong(1)
    val total_count2: Long = buffer2.getLong(1)

    //    各个城市总数相加
    buffer1(0) = city_count1.foldLeft(city_count2) { case (map, (city_name, count)) =>
      map + (city_name -> (map.getOrElse(city_name, 0L) + count))
    }

    //    总数相加
    buffer1(1) = total_count1 + total_count2
  }

  //  计算结果:北京21.2%，天津13.2%，其他65.6%
  override def evaluate(buffer: Row): String = {

    //    1.取出各城市点击次数及大区总数
    val city_count: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val total_count: Long = buffer.getLong(1)

    //    2.对城市点击数进行排序，并取出前两名
    val sortCityCountTop2: List[(String, Long)] = city_count.toList.sortWith { case (c1, c2) => c1._2 > c2._2 }.take(2)

    //    3.其他城市占比
    var otherRatio = 100D

    //    4.前两名city占比
    val city_ratioTop2: List[CityRatio] = sortCityCountTop2.map { case (city, count) =>
      val ratio: Double = Math.round(count.toDouble * 1000 / total_count) / 10D
      otherRatio -= ratio
      CityRatio(city, ratio)
    }
    val ratios: List[CityRatio] = city_ratioTop2 :+ CityRatio("其他：", Math.round(otherRatio * 10 / 10D))

    val str: String = ratios.mkString(",")
    str
  }
}
