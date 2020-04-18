package com.michael.es

import com.michael.base.BaseSparkSession
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer

/**
 * @author chudichen
 * @since 2020/4/17
 */
object EsDemo extends BaseSparkSession {

  def main(args: Array[String]): Unit = {
    query()
  }

  def write() = {
    val list = new ListBuffer[User]()
    list.append(User("Tom", 18))
    list.append(User("Annie", 20))
    val rdd = sc.makeRDD(list)
    EsSpark.saveToEs(rdd ,"/user/_doc")
  }

  def query() = {
    val query = "{\"query\":{\"match_all\":{}}}"
    val rdd = EsSpark.esJsonRDD(sc, "/user/_doc", query)
    rdd.collect().foreach(println)
  }

  case class User(name: String, age: Int)
}
