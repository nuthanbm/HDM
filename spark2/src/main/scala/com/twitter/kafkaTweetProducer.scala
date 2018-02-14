package com.twitter
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class kafkaTweetProducer(props: Properties) {
  val producer = new KafkaProducer[String, String](props)
  
  def sendMessage(topic: String, message:String){
    val data = new ProducerRecord[String,String](topic,null,message)
    producer.send(data)
  }
  
  def closeProducer(){
    producer.close()
  }
}