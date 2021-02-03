package com.singh.deepak.kakfa

import java.util
import java.util.{Collections, Properties, _}

import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{TimeoutException, WakeupException}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object KafkaUtil {

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)

  /*
  *  Check the Kafka Connection
  *  @param @cp We pass details in Map like zookeeper url, bootstrap server details
  *  @return Boolean
  * */

  // cp = Map("bootstrap.servers" -> "bootstrap server detail", "zookeeper.connect" -> "zookeeper detail")
  def checkKafkaConnection (cp: Map[String, Object]): Boolean = {

    val props = new Properties()
    props.put("bootstrap.servers", cp.getOrElse("bootstrap.servers", "localhost:9092"))
    props.put("group.id", "test-consumer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    try {
      val consumer = new KafkaConsumer[String, String](props)
      consumer.close()
      true
    } catch {
      case _: WakeupException => false
      case _: TimeoutException => false
      case _: KafkaException => false
      case _: Exception => false
    }
  }

  /*
 *  Create Kafka Topic
 *  @param @bootstrap  Bootstrap server details
 *  @param @topicName  Topic details
 *  @param @partition  Partition details
 *  @param @replication  Replication details
 *  
 * */
  def createTopic (bootstrap: String, topicName: String, partition: Int, replication: Int): Unit = {

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)

    val kafkaAdminClient = AdminClient.create(props)
    val topicExist = kafkaAdminClient.listTopics().names().get().contains(topicName)
    if (!topicExist) {
      logger.info(s" Kafka Topic $topicName doesn't exist ..!")
      val topic = new NewTopic(topicName, partition, replication)
      val creationStatus = kafkaAdminClient.createTopics(Collections.singleton(topic)).values()
      creationStatus.asScala.foreach(creationResult => {
        val name = creationResult._1
        Try(creationResult._2.get()) match {
          case Success(_) => logger.info(s" Topic $name created successfully ..!")
          case Failure(e) => e.printStackTrace()
        }
      })
    }
    logger.info(s" Topic $topicName already exist ...! ")
  }

  /*
*  Update Kafka Topic configuration
*  @param @bootstrap Bootstrap server details
*  @param @topicName Topic details
*
* */
  def updateTopic (bootstrap: String, topicName: String): Unit = {

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)

    val kafkaAdminClient = AdminClient.create(props)

    val resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName)
    val retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "1800000")
    val updateConfig = new util.HashMap[ConfigResource, Config]()
    updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)))
    logger.info(s" Updating Config for topic $topicName")
    val alterConfigsResult = kafkaAdminClient.alterConfigs(updateConfig).values()
    alterConfigsResult.asScala.foreach(updateResult => {
      Try(updateResult._2.get()) match {
        case Success(_) => logger.info(s" Kafka topic $topicName updated successfully")
        case Failure(ex) => ex.printStackTrace()
      }
    })
  }


  /*
  *  Describe Kafka Topic configuration
*  @param @bootstrap Bootstrap server details
*  @param @topicName Topic details
*
* */
  def describeTopic (bootstrap: String, topicName: String): Unit = {

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)

    val kafkaAdminClient = AdminClient.create(props)

    val resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName)
    val describeResult = kafkaAdminClient.describeConfigs(Collections.singleton(resource)).values()
    describeResult.asScala.foreach(verifyResult => {
      val retentionConfig = verifyResult._2.get().asInstanceOf[Config].get("retention.ms")
      logger.info(s" ${resource.name()} --> ${retentionConfig.name()} --> ${retentionConfig.value()}")
    })
  }
  
/*
*  Delete Kafka Topic
*  @param @bootstrap  Bootstrap server details
*  @param @topicName  Topic details
*
* */

  def deleteTopic (bootstrap: String, topicName: String): Unit = {

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)

    val kafkaAdminClient = AdminClient.create(props)
    // val topicExist = kafkaAdminClient.listTopics().names().get().contains(topicName)
    // if (!topicExist) {
    //   logger.error(s" Kafka Topic $topicName doesn't exist ..!")
    // }
    // logger.info(s" Topic $topicName already exist ...! ")
    val deleteTopicResult = kafkaAdminClient.deleteTopics(topicName)
    while( {
      !deleteTopicResult.all.isDone
    }){
      // wait for future task to complete
    }
  }
}
