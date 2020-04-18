package com.michael.etl

import java.time.{LocalDate, Month, ZoneId}
import java.util.Date

import com.michael.base.BaseSparkSession
import com.michael.caseclass.{Order, Reg}
import com.michael.util.{DateStyle, DateUtil}

/**
 * 每周ETL
 *
 * @author chudichen
 * @since 2020/4/16
 */
object WowEtl extends BaseSparkSession {

  def main(args: Array[String]): Unit = {
    val reg = regCount()
    val order = orderCount()
  }

  def regCount(): Array[Reg] = {
    var sql = "select date_format(create_time,'yyyy-MM-dd') as day," +
      " count(id) as regCount from spark_elasticsearch.t_member where create_time >='%s' " +
      " and create_time < '%s' group by date_format(create_time,'yyyy-MM-dd')"
    val oneWeekBefore = getOneWeekBefore()
    sql = String.format(sql, oneWeekBefore, oneWeekBefore)
    val df = session.sql(sql)
    df.rdd.map(row => Reg(row(0).toString, row(1).toString.toInt)).collect()
  }

  def orderCount(): Array[Order] = {
    var sql = "select date_format(create_time,'yyyy-MM-dd') as day," +
      " count(order_id) as orderCount from spark_elasticsearch.t_order where create_time >='%s' and create_time < '%s' " +
      " group by date_format(create_time,'yyyy-MM-dd')"
    val oneWeekBefore = getOneWeekBefore()
    sql = String.format(sql, oneWeekBefore, oneWeekBefore)
    val df = session.sql(sql)
    df.rdd.map(row => Order(row(0).toString, row(1).toString.toInt)).collect()
  }

  def getOneWeekBefore() = {
    val zoneId = ZoneId.systemDefault
    val now = LocalDate.of(2019, Month.NOVEMBER, 30)
    val nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant)
    val nowDayOne = DateUtil.addDay(nowDaySeven, -7)
    val lastDaySeven = DateUtil.addDay(nowDayOne, -7)
    DateUtil.DateToString(lastDaySeven, DateStyle.YYYY_MM_DD_HH_MM_SS)
  }
}
