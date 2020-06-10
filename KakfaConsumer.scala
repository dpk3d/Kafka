package com.singh.deepak.kakfa

import java.util._

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, ConsumnerRecords}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

object KafkaConsumer extends App {

    val topicName = "Deepak_Kafka_Test"
    val brokerId = "btm21:4321"
    val groupId = "test"
    
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "20000")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    
    val consumerKafka = new KafkaConsumer[(String,String)](properties)
    
    private val partition: Seq[TopicPartition] = Seq( new TopicPartition("Deepak_Kafka_Test","0"), 
           new TopicPartition("Deepak_Kafka_Test","1"), new TopicPartition("Deepak_Kafka_Test","2"))
           
    consumerKafka.assign(partition.asJava)
    
    partition.foreach{
      consumerKafka.seek(_, 14432)
      }
      
    while (true){
        val messages: ConsumerRecords[String, String] = consumerKafka.poll(1000)
        for (message <- messages.toIterator.asScala) {
              val record = message.value
              println(record)
              }
    }
    
   
}
