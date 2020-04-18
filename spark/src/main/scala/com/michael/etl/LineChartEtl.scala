package com.michael.etl

import java.time.{LocalDate, Month, ZoneId}
import java.util.Date

import com.michael.base.BaseSparkSession
import com.michael.util.{DateStyle, DateUtil}
import com.michael.vo.LineVO

import scala.collection.mutable.ListBuffer


/**
 * 折线图ETL
 *
 * @author chudichen
 * @since 2020/4/16
 */
//noinspection DuplicatedCode
object LineChartEtl extends BaseSparkSession {

  def main(args: Array[String]): Unit = {
    val lineVOs = lineEtl()
    lineVOs.foreach(println)
  }

  def lineEtl(): Array[LineVO] = {
    var memberSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
      " count(*) as regCount, max(id) as memberCount" +
      " from spark_elasticsearch.t_member where create_time >= '%s'" +
      " group by date_format(create_time,'yyyy-MM-dd') order by day"

    val sevenDayBefore = getSevenDayBefore()
    memberSql = String.format(memberSql, sevenDayBefore)
    val memberDF = session.sql(memberSql)

    var orderSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
      " max(order_id) as order, sum(origin_price) as gmv" +
      " from spark_elasticsearch.t_order where create_time >= '%s'" +
      " group by date_format(create_time,'yyyy-MM-dd') order by day"
    orderSql = String.format(orderSql, sevenDayBefore)
    val orderDF = session.sql(orderSql)

    // Join
    val memberJoin = orderDF.join(memberDF, memberDF.col("day").equalTo(orderDF.col("day")), "inner")
    // 获取VO集合
    val lineVOs = memberJoin.rdd.map(row => {
      LineVO( row.get(0).toString, row.get(4).toString.toLong, row.get(5).toString.toLong, row.get(1).toString.toLong, row.get(2).toString.toDouble)
    }).collect()

    var gmvTotalSql = "select sum(origin_price) as totalGmv from spark_elasticsearch.t_order where create_time < '%s'"
    gmvTotalSql = String.format(gmvTotalSql, sevenDayBefore)
    val gmvAll = session.sql(gmvTotalSql).rdd.first().get(0).toString.toDouble

    val destList= new ListBuffer[Double]
    for (i <- lineVOs.indices) {
      val lineVO = lineVOs.apply(i)
      val gmv = lineVO.gmv
      var temp = gmv + gmvAll

      for (j <- 0 until i) {
        val prev = lineVOs.apply(j)
        temp += prev.gmv
      }
      destList.append(temp)
    }

    for (i <- destList.indices) {
      val lineVO = lineVOs.apply(i)
      lineVO.gmv = destList.apply(i)
    }
    lineVOs
  }

  /**
   * 获取7天前日期（为了配合测试的数据）
   *
   * @return
   */
  def getSevenDayBefore(): String = {
    val defaultZoneId = ZoneId.systemDefault
    val now = LocalDate.of(2019, Month.NOVEMBER, 30)
    val nowDay = Date.from(now.atStartOfDay(defaultZoneId).toInstant)
    val sevenDayBefore = DateUtil.addDay(nowDay, -8)
    DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS)
  }

}
