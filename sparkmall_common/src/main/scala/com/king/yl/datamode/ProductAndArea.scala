package com.king.yl.datamode


/***
  * 华北	商品A	100000	北京21.2%，天津13.2%，其他65.6%
  * 华北	商品P	80200	北京63.0%，太原10%，其他27.0%
  * 华北	商品M	40000	北京63.0%，太原10%，其他27.0%
  *
  * select 	a.date, a.session_id, a.click_product_id, c.product_name, a.city_id, b.city_name, b.area
  * from user_visit_action a
  * join  city_info b
  * on a.city_id=b.city_id
  * join  product_info c
  * on a.click_product_id=c.product_id
  */
case class ProductAndArea (
                            date: String,
                            session_id: String,
                            click_product_id: Long,
                            product_name: String,
                            city_id:Long,
                            city_name:String,
                            area:String
                          )