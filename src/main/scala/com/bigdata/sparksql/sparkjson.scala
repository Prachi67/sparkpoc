package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkjson {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkjson").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkjson").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
val data="file:///C:\\BigData\\datasets\\world_bank.json"
    val df=spark.read.format("json").load(data)
  //  df.printSchema()
// clean data
df.createOrReplaceTempView("tab")
    val res  = spark.sql("select  _id.`$oid` OID, approvalfy,board_approval_month,boardapprovaldate, borrower,closingdate,country_namecode,countrycode, supplementprojectflg,countryname,countryshortname,envassesmentcategorycode,grantamt,ibrdcommamt,id,idacommamt,impagency,lendinginstr, lendinginstrtype,lendprojectcost, mjthemecode,sector.name[0] name1,sector.name[1] name2, sector.name[2] name3,sector1.name S1Name, sector1.percent S1Percent,sector2.name S2Name, sector2.percent S2Percent,sector3.name S3Name, sector3.percent S3Percent,sector4.name S4Name, sector4.percent S4Percent, prodline, prodlinetext, productlinetype,mjtheme[0] mjtheme1,mjtheme[1] mjtheme2,mjtheme[2] mjtheme3  , project_name ,projectfinancialtype, projectstatusdisplay,regionname,sectorcode, source,status,themecode,totalamt,totalcommamt,url,mp.Name mpname, mp.Percent mppercent, tn.code tncode, tn.name tnname,mn.code mncode, mn.name mnname, tc.code tccode, tc.name tcname, pd.DocDate pddocdate ,pd.DocType pddoctype, pd.DocTypeDesc pddoctypedesc, pd.DocURL  pddocurl, pd.EntityID pdid from tab lateral view explode(majorsector_percent) tmp as mp lateral view explode(theme_namecode) tmp as tn lateral view explode(mjsector_namecode) tmp as mn lateral view explode(mjtheme_namecode) tmp as tc lateral view explode(projectdocs) tmp as pd")
    res.printSchema()
    res.createOrReplaceTempView("tab1")


    //clean process
 /*   df.createOrReplaceTempView("tab")
    val res1 = spark.sql("select _id id, city, loc[0] lang, loc[1] lati, pop, state from tab")
    res1.createOrReplaceTempView("tab1")
    val res = spark.sql("select state, count(*) cnt from tab1 group by state order by cnt desc")
    res.show()
    res.printSchema()*/
    spark.stop()
  }
}