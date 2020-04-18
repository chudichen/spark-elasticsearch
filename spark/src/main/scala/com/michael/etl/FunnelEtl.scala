package com.michael.etl

import com.michael.base.BaseSparkSession
import com.michael.vo.{FunnelVO, MemberVO}

/**
 * 漏斗图ETL
 *
 * @author chudichen
 * @since 2020/4/16
 */
object FunnelEtl extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    // 订购
    val orderMember = session.sql("select distinct(member_id) from spark_elasticsearch.t_order where order_status = 2")
    // 复购
    val orderAgain = session.sql("select * from " +
      "(select member_id, count(member_id) as memberCount from spark_elasticsearch.t_order " +
      "where order_status = 2 group by member_id) " +
      "as t where t.memberCount > 1")
    // 正在付款
    val charge = session.sql("select distinct(member_id) from spark_elasticsearch.t_order where order_status = 1")
    val orderJoin = charge.join(orderAgain, orderAgain.col("member_id").equalTo(charge.col("member_id")), "inner")

    val orderCount = orderMember.count()
    val orderAgainCount = orderAgain.count()
    val chargeCoupon = orderJoin.count()

    // 漏斗图VO数据封装
    FunnelVO(1000L, 800L, 600L, orderCount, orderAgainCount, chargeCoupon)
  }
}
