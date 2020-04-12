package com.michael

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Michael Chu
  * @since 2020-03-26 11:05
  */
trait BaseContext {

  val conf: SparkConf = new SparkConf()
//    .set("", "")
    .setMaster("local[2]")
    .setAppName("Spark App")
  val sc = new SparkContext(conf)

  val path: String = new File(".").getCanonicalPath
  // Set work dir
  // File workaround = new File(".");
  System.getProperties.put("hadoop.home.dir", path)
  new File("./spark/bin").mkdirs()
  new File("./spark/bin/winutils.exe").createNewFile()
}
