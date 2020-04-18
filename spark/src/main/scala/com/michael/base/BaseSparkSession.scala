package com.michael.base

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author Michael Chu
  * @since 2020-04-08 14:10
  */
trait BaseSparkSession {

  System.setProperty("HADOOP_USER_NAME","chu")
  val session: SparkSession = SparkSession.builder()
    .config("es.nodes", "namenode")
    .config("es.port", "9200")
    .config("es.batch.size.entries", "1")
    .config("es.batch.write.retry.count", "10")
    .config("es.batch.write.retry.wait", "100")
    .config("es.http.timeout", "100s")
    .config("es.index.auto.create", true.toString)
    .config("spark.sql.catalogImplementation", "in-memory")
    .appName("Spark session")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  val sc: SparkContext = session.sparkContext
}
