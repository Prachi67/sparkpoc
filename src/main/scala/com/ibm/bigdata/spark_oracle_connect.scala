package com.ibm.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object spark_oracle_connect {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("spark_oracle_connect").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("spark_oracle_connect").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
//val data = "C:\\Work\\dataset\\bank-full.csv"
    //val bankdf = spark.read.format(source ="csv").option("header","true").option("delimiter",";").option("inferschema","true").load(data)
        val url="jdbc:oracle:thin:@//oracl.c0tyb2ngszqg.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop =new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df=spark.read.jdbc(url,"EMP",prop)
    df.show()
    spark.stop()
  }
}