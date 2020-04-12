package com.michael.etl

import com.michael.BaseContext

/**
  * @author Michael Chu
  * @since 2020-03-26 11:04
  */
object HotWordEtlWithScala extends BaseContext{

  def main(args: Array[String]): Unit = {
    val path = "hdfs://namenode:8020/data/spark-demo/sougou.txt"
    val rdd = sc.textFile(path)
    println("------count--------:" + rdd.count())
  }

}
