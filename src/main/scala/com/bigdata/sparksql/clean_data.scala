package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object clean_data {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("clean_data").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("clean_data").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
val data="file:///C:\\BigData\\datasets\\100000Records.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.printSchema()
   // df.createOrReplaceTempView("tab")
    val cols= df.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))
    val ndf=df.toDF(cols:_*)
    //toDF used to rename all columns
    //:_* refers if columns in the form of array that array columns appended to existing dataframe.
    ndf.createOrReplaceTempView("tab1")
   val res = spark.sql("select DateofJoining from tab1 ")

    //functions

    res.show()
    spark.stop()
  }
}