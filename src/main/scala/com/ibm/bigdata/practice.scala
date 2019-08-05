package com.ibm.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object practice {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("practice").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("practice").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val crdd= sc.textFile("C:\\Work\\dataset\\us-500.csv")
    val head = crdd.first()
    val splitcoma = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val pro = crdd.filter(x=>x!=head).map(x=>x.split (splitcoma)).map (x=>(x (0),x(9)))
    pro.take(num =10).foreach(println)

    import spark.implicits._
    import spark.sql
    //this is df practice
//val input ="C:\\Work\\dataset\\us-500.csv"
   // val df = spark.read.format(source = "csv").option ("header", "true").option("inferSchema", "true").load(input)
   // df.createOrReplaceTempView(viewName = "tab")
   // val res = spark.sql("select count (*) from tab")
   // res.show()
   // val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input)
    //this is RDD practice

    spark.stop()
  }
}