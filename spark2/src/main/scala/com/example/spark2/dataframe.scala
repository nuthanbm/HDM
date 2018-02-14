package com.example.spark2
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType,StringType,StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory

object datafme {
  def main(args: Array[String]) {
    // val appconf = ConfigFactory.load()
  val conf = new SparkConf().setAppName("wordcount").setMaster("local")
  val sc = new SparkContext(conf)
  val appconf = ConfigFactory.load()
  val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
  import spark.implicits._
  // Below CODE to read the data from the properties file and create a DataFrame.
  val peopleRDD = spark.sparkContext.textFile("D:/emp.txt")
  val peopleRDD1 = spark.sparkContext.textFile("D:/dq.txt")
 /* val schemaString = appconf.getString("atti")
  val datatypes = appconf.getString("types")
 // val types = datatypes.split(" ")
  val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
 // .map(fieldName => StructField(fieldName, IntegerType, nullable = true))
  
  val schema = StructType(fields)
  
  val rowRDD = peopleRDD
 .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))
  
  val peopleDF = spark.createDataFrame(rowRDD, schema)*/
  
 // peopleDF.show
  //peopleDF.write.format("com.databricks.spark.csv").save("D:/dfout")
  //Below code is other version to read the data from the properties file and create the table.

  val schema = new StructType()
  .add(StructField(appconf.getString("att1"), IntegerType, true))
  .add(StructField(appconf.getString("att2"), StringType, true))
  .add(StructField(appconf.getString("att3"), IntegerType, true))
  .add(StructField(appconf.getString("att4"), StringType, true))
  
  val rowRDD = peopleRDD
 .map(_.split(","))
  .map(attributes => Row(attributes(0).toInt, attributes(1),attributes(2).toInt,attributes(3)))
  
  val peopleDF = spark.createDataFrame(rowRDD, schema)
  
  val dq_schema = new StructType()
  .add(StructField(appconf.getString("dq_id"), IntegerType, true))
  .add(StructField(appconf.getString("stat"), StringType, true))
  
  val dq_row = peopleRDD1.map(_.split(",")).map(attributes => Row(attributes(0).toInt,attributes(1)))
  
  val dqDF = spark.createDataFrame(dq_row, dq_schema)
  
  val joinDF = peopleDF.join(dqDF, peopleDF(appconf.getString("att1")) === dqDF(appconf.getString("dq_id")))
  
  //peopleDF.show
  //dqDF.show()
  
 joinDF.show
  
  
  }
}