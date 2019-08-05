package com.ibm.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object map_filter {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("map_filter").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("map_filter").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
val cardd =sc.textFile ("file:C:\\Work\\dataset\\ca-500.csv")
    val head = cardd.first()
    val splitcoma = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val pro =cardd.filter(x=>x!=head).map(x=>x.split (splitcoma)).map (x=>(x(0),x(5)))
    pro.take(num =10).foreach(println)
    spark.stop()
  }
}