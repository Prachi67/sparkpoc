package com.ibm.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object data_functions {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("data_functions").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("data_functions").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val df = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
//get current date
//val ndf = df.withColumn("today",current_date())
    //current timestamp
 //   val ndf = df.withColumn("today",current_timestamp())

    //timestamp convert to date.. to_date used to convert string/timestamp format to date format.
    val ndf = df.withColumn("today",to_date(current_timestamp())).withColumn("hiredate", to_date(unix_timestamp($"hiredate","dd-MMM-yy").cast("timestamp")))
        // Now problem is column convert to date format, its ok, but 99% you will get nulls the main reason date not dd-mm-yyyy format its dd-mmm-yyyy format thats why convert date dd-mmm-yyyy to dd-mm-yyyy format

    //date diff used to know days between two days ...
    //val ndf1 = ndf.withColumn("datediff",datediff($"today",$"hiredate")).withColumn("date_add", date_add($"today",100))
      // above line add date used to know after 100 days whats date.
    //if you add -100 means 100 days before or u can use date_sub
    //.withColumn("date_add", date_add($"today",-100)) or
    //.withColumn("date_sub", date_sub($"today",100))
    //val ndf1 = ndf.withColumn("day",dayofmonth($"hiredate")).withColumn("dayofyear",dayofyear($"hiredate"))
    //dayofmonth means get only day from specified date. day of year means from january 1 to today date how many days completed get int format.
    //val ndf1 = ndf.withColumn("lastday",last_day($"hiredate")).withColumn("monthbetween",months_between($"today",$"hiredate"))

    val ndf1 = ndf.withColumn("nextsunday",next_day($"today","Su")).withColumn("trunc",trunc($"hiredate","YYYY")).withColumn("lastdate",last_day($"hiredate")).withColumn("test",weekofyear($"hiredate"))
    //unix timestamp means



    spark.stop()
  }
}