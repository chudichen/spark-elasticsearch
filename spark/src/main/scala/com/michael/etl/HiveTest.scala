package com.michael.etl

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author Michael Chu
  * @since 2020-04-08 15:27
  */
object HiveTest {

  def main(args: Array[String]): Unit = {
    val path: String = new File(".").getCanonicalPath
    // Set work dir
    // File workaround = new File(".");
    System.getProperties.put("hadoop.home.dir", path)
    new File("./spark/bin").mkdirs()
    new File("./spark/bin/winutils.exe").createNewFile()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      //.setMaster("local[*]")
      .setMaster("local[*]")
    val session = SparkSession.builder()
      .config(conf)
      // 指定hive的metastore的端口  默认为9083 在hive-site.xml中查看
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      //指定hive的warehouse目录
//      .config("spark.sql.warehouse.dir", "hdfs://192.168.40.51:9000/user/hive/warehouse")
      //直接连接hive
      .enableHiveSupport()
      .getOrCreate()
  }
}
