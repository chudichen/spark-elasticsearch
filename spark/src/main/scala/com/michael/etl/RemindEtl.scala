package com.michael.etl

import java.util.Date

import com.michael.util.{DateStyle, DateUtil}
import java.time.LocalDate
import java.time.Month
import java.time.ZoneId

import com.michael.BaseSparkSession
import com.michael.caseclass.{CouponReminder, FreeReminder}

/**
 * 超时提醒ETL
 *
 * @author chudichen
 * @since 2020/4/16
 */
//noinspection DuplicatedCode
object RemindEtl extends BaseSparkSession {

  def main(args: Array[String]): Unit = {
    val freeReminders = freeReminderEtl()
    val couponReminders = couponReminderEtl()
  }

  def freeReminderEtl():Array[FreeReminder] = {
    val eightDayBefore = getEightBefore()
    var sql = "select date_format(create_time, 'yyyy-MM-dd') as day, count(member_id) as freeCount " +
      "from spark_elasticsearch.t_coupon_member where coupon_id = 1 " +
      "and coupon_channel = 2 and create_time >= '%s' " +
      "group by date_format(create_time, 'yyyy-MM-dd') order by day desc"
    sql = String.format(sql, eightDayBefore)
    val df = session.sql(sql)
    df.rdd.map(row => FreeReminder(row(0).toString, row(1).toString.toInt)).collect()
  }

  def couponReminderEtl(): Array[CouponReminder] = {
    val eightDayBefore = getEightBefore()
    var sql = "select date_format(create_time, 'yyyy-MM-dd') as day, count(member_id) as couponCount " +
      "from spark_elasticsearch.t_coupon_member where coupon_id != 1 " +
      "and create_time >= '%s' " +
      "group by date_format(create_time,'yyyy-MM-dd')"
    sql = String.format(sql, eightDayBefore)
    val df = session.sql(sql)
    df.rdd.map(row => CouponReminder(row(0).toString, row(1).toString.toInt)).collect()
  }

  /**
   * 获取8天前的日期
   *
   * @return 8 day before
   */
  private def getEightBefore(): String = {
    val zoneId = ZoneId.systemDefault
    val now = LocalDate.of(2019, Month.NOVEMBER, 30)
    val nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant)

    // 优惠券 8 天失效的，所以需要加一天
    val tomorrow = DateUtil.addDay(nowDaySeven, 1)
    val pickDay = DateUtil.addDay(tomorrow, -8)
    DateUtil.DateToString(pickDay, DateStyle.YYYY_MM_DD_HH_MM_SS)
  }
}
