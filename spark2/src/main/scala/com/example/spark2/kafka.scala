package com.example.spark2

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils

object kafka {
  
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = KafkaUtils.createStream(ssc,"localhost:2181","consumer-group",Map("demo"->5))
    lines.print()
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}