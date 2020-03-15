package com.spark.test
import java.util.Properties

import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("yarn-cluster")
      .appName("HdfsTest")
      .getOrCreate()

//    val filePath="E:\\大数据资料\\大数据项目\\新闻实时分析\\Windows开发环境配置文件-Spark\\stu.txt"

//    val rdd=spark.sparkContext.textFile(filePath)
//    val lines =rdd.flatMap(x=>x.split(" "))
//      .map(x=>(x,1))
//      .reduceByKey((a,b)=>(a+b))
//      .collect().toList
    // println(lines)
    val filePath=args(0)
    import spark.implicits._
    val dataSet = spark.read.textFile(filePath)
      .flatMap(x=>x.split(" "))
      .map(x=>(x,1)).groupBy("_1").count().show()




  }

}
