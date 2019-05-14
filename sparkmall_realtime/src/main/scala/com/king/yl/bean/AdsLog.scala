package com.king.yl.bean

/***
  *
  * @param timestamp
  * @param area
  * @param city
  * @param userid
  * @param adid
  */
case class AdsLog(
                   timestamp: Long,
                   area: String,
                   city: String,
                   userid: String,
                   adid: String
                 )
