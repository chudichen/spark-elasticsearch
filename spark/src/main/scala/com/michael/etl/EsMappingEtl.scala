package com.michael.etl

import com.michael.base.BaseSparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
 * ES Mapping 清洗
 *
 * @author chudichen
 * @since 2020/4/17
 */
object EsMappingEtl extends BaseSparkSession {

  def main(args: Array[String]): Unit = {
    etl()
  }

  def etl() = {
    // member数据
    session.sql("select id as memberId,phone,sex,member_channel as channel,mp_open_id as subOpenId," +
      "address_default_id as address,date_format(create_time,'yyyy-MM-dd') as regTime " +
      "from spark_elasticsearch.t_member").createOrReplaceTempView("member")

    // 订单商品
    session.sql("select o.member_id as memberId, " +
      "date_format(max(o.create_time),'yyyy-MM-dd') as orderTime," +
      "count(o.order_id) as orderCount," +
      "collect_list(DISTINCT oc.commodity_id) as favGoods, " +
      "sum(o.pay_price) as orderMoney " +
      "from spark_elasticsearch.t_order as o left join spark_elasticsearch.t_order_commodity as oc " +
      "on o.order_id = oc.order_id group by member_id").createOrReplaceTempView("oc")

    // 免费优惠卷
    session.sql("select member_id as memberId, " +
      "date_format(create_time,'yyyy-MM-dd') as freeCouponTime " +
      "from spark_elasticsearch.t_coupon_member where coupon_id = 1").createOrReplaceTempView("freeCoupon")

    // 优惠卷时间
    session.sql("select member_id as memberId, " +
      "collect_list(date_format(create_time,'yyyy-MM-dd')) as couponTimes " +
      "from spark_elasticsearch.t_coupon_member where coupon_id !=1 group by member_id").createOrReplaceTempView("couponTimes")

    // 付款金额
    session.sql("select cm.member_id as memberId , sum(c.coupon_price/2) as chargeMoney " +
      "from spark_elasticsearch.t_coupon_member as cm left join spark_elasticsearch.t_coupon as c " +
      "on cm.coupon_id = c.id where cm.coupon_channel = 1 group by cm.member_id").createOrReplaceTempView("chargeMoney")

    // 送达时间
    session.sql("select (to_unix_timestamp(max(arrive_time)) - to_unix_timestamp(max(pick_time))) " +
      "as overTime, member_id as memberId " +
      "from spark_elasticsearch.t_delivery group by member_id").createOrReplaceTempView("overTime")

    // 反馈
    session.sql("select fb.feedback_type as feedback,fb.member_id as memberId " +
      "from spark_elasticsearch.t_feedback as fb " +
      "left join (select max(id) as mid,member_id as memberId " +
      "from spark_elasticsearch.t_feedback group by member_id) as t " +
      "on fb.id = t.mid").createOrReplaceTempView("feedback")

    val result = session.sql("select m.*,o.orderCount,o.orderTime,o.orderMoney,o.favGoods," +
      " fb.freeCouponTime,ct.couponTimes, cm.chargeMoney,ot.overTime,f.feedBack" +
      " from member as m " +
      " left join oc as o on m.memberId = o.memberId " +
      " left join freeCoupon as fb on m.memberId = fb.memberId " +
      " left join couponTimes as ct on m.memberId = ct.memberId " +
      " left join chargeMoney as cm on m.memberId = cm.memberId " +
      " left join overTime as ot on m.memberId = ot.memberId " +
      " left join feedback as f on m.memberId = f.memberId ")

    EsSparkSQL.saveToEs(result, "/tag/_doc")
  }

}
