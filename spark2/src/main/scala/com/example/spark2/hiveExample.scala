package com.example.spark2
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
object hiveExample {
  def main(args: Array[String]){
    /*
    val warehouselocation = new File("spark-warehouse").getAbsolutePath
    val conf = new SparkConf().setAppName("HiveExample").setMaster("local")
  val sc = new SparkContext(conf)
   val appconf = ConfigFactory.load()
    val spark = SparkSession.builder().appName("hive-example")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate()*/
    
     val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = KafkaUtils.createStream(ssc,"localhost:2181","demo",Map("customer"->5))
    lines.print()
    ssc.start()
    ssc.awaitTermination()
    
    
  //  import spark.implicits._
   // import spark.sql
    
    //sql("CREATE TABLE QPNY_MBR_MBR_DEMO(FNAME STRING,LNAME STRING,SAL INT,DEPT STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
   //sql("LOAD DATA LOCAL INPATH 'C:/examples/src/main/resources/empl.txt' INTO TABLE QPNY_MBR_MBR_DEMO")
  // val qry = sql(appconf.getString("query"))
  // qry.show()
  //  val tablestring = appconf.getString("tableName")
   // val tables = tablestring.split(" ")
  //  val count = tables.length;
 //sql(appconf.getString("query2"))
  // sql(appconf.getString("query3"))
 //sql(appconf.getString("query")).show()
    /*sql("CREATE TABLE MEMBER(OWNER STRING,SAL INT,DEPT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql("INSERT OVERWRITE TABLE MEMBER SELECT CONCAT(FNAME,LNAME),SAL,DEPT FROM emp7")
    sql("SELECT * FROM MEMBER").show()*/
    
    
  }
}