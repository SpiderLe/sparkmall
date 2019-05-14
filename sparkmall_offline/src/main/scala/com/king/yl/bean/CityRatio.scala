package com.king.yl.bean

case class CityRatio (city: String, ratio:Double) {

  override def toString: String = s"$city: $ratio%"
}
