package com.michael

import org.apache.spark.sql.SparkSession

/**
  * @author Michael Chu
  * @since 2020-04-08 14:10
  */
trait BaseSparkSession {

  System.setProperty("HADOOP_USER_NAME","chu")
  val session:SparkSession = SparkSession.builder()
    .config("spark.sql.catalogImplementation", "in-memory")
    .appName("Spark session")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
}
