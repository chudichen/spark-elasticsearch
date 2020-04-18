package com.michael.etl

import com.michael.base.BaseSparkSession
import com.michael.caseclass.{MemberChannel, MemberHeat, MemberMpSub, MemberSex}
import com.michael.vo.MemberVO

/**
 * 用户性别ETL
 * @author chudichen
 * @since 2020-04-16
 */
object MemberEtl extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val memberChannels = memberChannelEtl()
    val memberSexes = memberSexEtl()
    val memberMpSub = memberMpSubEtl()
    val memberHeat = memberHeatEtl()
    val memberVO = MemberVO(memberChannels, memberSexes, memberMpSub, memberHeat)
    println(memberVO)
  }

  /**
   * 性别统计ETL
   *
   * @return 性别
   */
  def memberSexEtl(): java.util.List[MemberSex] = {
    val df = session.sql("select sex as memberSex, count(id) as sexCount " +
      "from spark_elasticsearch.t_member group by sex")
    import session.implicits._
    df.map(row => MemberSex(row.get(0).toString.toInt,row.get(1).toString.toLong)).collectAsList()
  }

  /**
   * 用户登录方式统计ETL
   *
   * @return 用户登录方式
   */
  def memberChannelEtl(): java.util.List[MemberChannel] = {
    val df = session.sql("select member_channel, count(*) as sexCount " +
      "from spark_elasticsearch.t_member group by member_channel")
    import session.implicits._
    df.map(row => MemberChannel(row.get(0).toString.toInt, row.get(1).toString.toLong)).collectAsList()
  }

  /**
   * 微信订阅统计
   *
   * @return 订阅信息
   */
  def memberMpSubEtl() = {
    val row = session.sql("select count(mp_open_id) as subCount, " +
      "count(if(mp_open_id is null, 1, null)) as unSubCount from spark_elasticsearch.t_member").rdd.first()
    MemberMpSub(row.get(0).toString.toLong, row.get(1).toString.toLong)
  }

  /**
   * 用户热度
   *
   * @return 用户热度
   */
  def memberHeatEtl(): MemberHeat = {
    // 注册信息完整
    val reg_complete = session.sql("select count(phone=='Null') as reg, count(phone!='NULL') " +
      "as complete from spark_elasticsearch.t_member")
    // 再次购买
    val order_again = session.sql("select count(if(t.orderCount = 1, 1, null)) as order, " +
      "count(if(t.orderCount > 1, 1, null)) as orderAgain from " +
      "(select member_id, count(*) as orderCount from spark_elasticsearch.t_order group by member_id) as t")
    // 优惠卷
    val coupon = session.sql("select count(distinct member_id) as coupon from spark_elasticsearch.t_coupon_member")
    // 合并
    val join = coupon.crossJoin(reg_complete).crossJoin(order_again)
    join.rdd.map(row => MemberHeat(row.get(0).toString.toInt,
      row.get(1).toString.toInt, row.get(2).toString.toInt,
      row.get(3).toString.toInt, row.get(4).toString.toInt)).first()
  }

}
