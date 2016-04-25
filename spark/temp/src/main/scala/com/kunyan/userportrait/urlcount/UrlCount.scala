package com.kunyan.userportrait.urlcount

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pc on 2016/4/21.
  * 统计URL中有效字段，统计，排序
  * @param source 数据读取地址
  * @param sc  SparkContext对象
  * @param file  文件保存地址
  * @author  zhangruibo
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    val source = args(0)
    val file = args(1)
    val conf = new SparkConf().setAppName("GetData")
    val sc = new SparkContext(conf)
    sc.textFile(source).map(_.split('\t')).filter(_.length >= 5)
      .map(x => hostCorrect(x(3))).filter(_._1 != "")
      .reduceByKey(_ + _).repartition(1)
      .sortBy(_._2,ascending = false)
      .map(x => x._1 + "\t" + x._2).saveAsTextFile(file)

    sc.stop()
  }

  def hostCorrect(arr: String): (String, Int) = {
    var host = ""
    if (arr != "NoDef" && arr != "") {
      if (arr.startsWith("http")) {       
          host += arr.substring(arr.indexOf(":") + 3, arr.length).split("/")(0)       
      } else {        
          host += arr.split("/")(0)       
      }
    }
    (host,1)
  }

}
