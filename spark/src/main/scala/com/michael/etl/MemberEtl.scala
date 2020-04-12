package com.michael.etl

import com.michael.BaseSparkSession

/**
  * @author Michael Chu
  * @since 2020-04-08 14:12
  */
object MemberEtl extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val databases = session.sql("show databases")
    databases.show()
  }
}
