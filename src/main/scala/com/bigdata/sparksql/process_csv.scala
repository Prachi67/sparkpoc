package com.bigdata.sparksql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
//context: a nutshell to create an API
//sparkContext ..sc...to create rdd
//SqlContext ...to create dataframe api
//sparkSession ... spark...to create dataset api
// in spark 3.0 no rdds
object process_csv {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("process_csv").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("process_csv").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
val input ="C:\\Work\\dataset\\us-500.csv"
    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input)
    // 9+9...18
    //"9"+"9"=99
  //  df.show() // by default show display top 20 records.
  //  df.printSchema()
    //sql friendly
    df.createOrReplaceTempView("tab") //allows to run sql queries on top of dataframe
  //  val res = spark.sql("select state,count(*) cnt from tab group by state order by cnt desc")
    //val res = spark.sql("select * from tab where zip =(select max(zip) from tab) ")
    val res = spark.sql("select first_name, replace(phone1,'-','') phone, replace(phone2,'-','') mobile from tab")

    res.show()
   // val op = "file:///C:\\Work\\dataset\\result"
    val op = "hdfs://localhost:9000/results"
    res.write.format("csv").option("header","true").option("delimiter","|").save(op)
//scala friendly
    /*val res = df.groupBy($"state").count().orderBy($"count".desc)
    res.show()*/
    spark.stop()
  }
}