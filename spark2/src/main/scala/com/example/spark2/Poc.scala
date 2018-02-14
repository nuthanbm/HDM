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

object Poc {
  def main(args : Array[String]){
    val appconf = ConfigFactory.load()
    val conf = new SparkConf().setAppName("poc").setMaster("local")
  //  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
   // val spark = SparkSession.builder().getOrCreate()
   val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
  import spark.implicits._
    val sc = new SparkContext(conf)
    
    //val emp =  sc.textFile("D:/emp.txt")
    val emp = spark.sparkContext.textFile("D:/emp.txt")
    
    /*val schema = new StructType()
  .add(StructField(appconf.getString("attributes.att1"), StringType, true))
  .add(StructField(appconf.getString("attributes.att2"), IntegerType, true))
  .add(StructField(appconf.getString("attributes.att3"), IntegerType, true))
  .add(StructField(appconf.getString("attributes.att4"),StringType,true))*/
    val schema = new StructType()
  .add(StructField("id", IntegerType, true))
  .add(StructField("name", StringType, true))
  .add(StructField("sal", IntegerType, true))
  .add(StructField("dept", StringType, true))
 // def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame
  /*def dfSchema(columnNames: List[String]): StructType = 
StructType( Seq( StructField(name = "id", dataType = IntegerType, nullable = false), 
StructField(name = "name", dataType = StringType, nullable = false), 
StructField(name = "sal", dataType = IntegerType, nullable = false),
StructField(name = "dept", dataType = StringType, nullable = false)) )*/
def row(line: List[String]): Row = Row(line(0).toInt, line(1),line(2).toInt,line(3))
//val rdd: RDD[String] = ...
//val schema = dfSchema(List("id", "name","sal","dept"))
  /*val sqlContext = new SQLContext(sc)
   import sqlContext.implicits._
  def row(line: List[String]): Row = Row(line(0), line(1).toInt,line(2).toInt,line(3))*/
val data = emp.map(_.split(",").to[List]).map(row)

  val df = spark.createDataFrame(data,schema)
  df.show
 
   // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   // val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
   // import sqlContext._
   // import sqlContext.implicits._
  /* val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    case class empl(id:Int,name:String,sal:Int,dept:String)
    val dframe = emp.map(_.split(",")).map(row => empl(row(0).toInt,row(1),row(2).toInt,row(3))).toDF()*/
    
  }
  
}