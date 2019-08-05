package com.ibm.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object diamond {
  case class uscc(carat:Float,cut:String,color:String,clarity:Float,depth:Float,table:Int,price:Int,x:Float,y:Float,z:Float)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("diamond").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("diamond").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val input="file:///C:\\Work\\dataset\\diamonds.csv)
  //  val srdd = sc.textFile(input)
  //  val head = srdd.first()
   // val rdd_cleaned = srdd.filter(x=>x!=head).map(x=>x.split (regex = ",")).map(x=>uscc(x(1).replaceAll(regex = "/"",replacement = "")
   // val df = rdd_cleaned.toDF()
  //  df.printSchema()
  //  df.createOrReplaceTempView(viewName = "tab")
  //  val res = spark.sql("select * from tab where price =326")
   // res.show()

    //pro.take(num =10).foreach(println)


    spark.stop()
  }
}