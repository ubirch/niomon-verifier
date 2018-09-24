package com.ubirch.signatureverifier

import akka.Done
import akka.kafka.scaladsl.Consumer
import cakesolutions.kafka.testkit.KafkaServer
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.ubirch.kafkasupport.MessageEnvelope
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetResetStrategy}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._

class RoutingTest extends FunSuite with Matchers with BeforeAndAfterAll {


  test("messages are routed to the topic 'valid' or 'invalid' depending on signature validity") {
    producer.send(MessageEnvelope.toRecord("incoming", "foo", MessageEnvelope("valid signature")))
    producer.send(MessageEnvelope.toRecord("incoming", "bar", MessageEnvelope("invalid signature")))

    validTopicConsumer.subscribe(List("valid").asJava)
    invalidTopicConsumer.subscribe(List("invalid").asJava)

    val validTopicRecords: ConsumerRecords[String, String] = validTopicConsumer.poll(5000)
    val invalidTopicRecords: ConsumerRecords[String, String] = invalidTopicConsumer.poll(5000)

    validTopicRecords.count() should be(1)
    invalidTopicRecords.count() should be(1)

    val approvedMessage = MessageEnvelope.fromRecord(validTopicRecords.iterator().next())
    val rejectedMessage = MessageEnvelope.fromRecord(invalidTopicRecords.iterator().next())


    approvedMessage.payload should equal("valid signature")
    rejectedMessage.payload should equal("invalid signature")
  }

  val kafkaServer = new KafkaServer(9992)
  val producer = createProducer(kafkaServer.kafkaPort)
  val invalidTopicConsumer = createConsumer(kafkaServer.kafkaPort, "1")
  val validTopicConsumer = createConsumer(kafkaServer.kafkaPort, "2")
  var stream: Consumer.DrainingControl[Done] = _

  override def beforeAll(): Unit = {
    kafkaServer.startup()
    createTopics("incoming", "invalid", "valid")
    stream = verifierStream.run()
  }
  override def afterAll(): Unit = {
    stream.shutdown().onComplete(_ => {
      producer.close()
      invalidTopicConsumer.close()
      validTopicConsumer.close()
      kafkaServer.close()
    })
  }


  private def createConsumer(kafkaPort: Int, groupId: String) = {
    KafkaConsumer(
      KafkaConsumer.Conf(new StringDeserializer(),
                         new StringDeserializer(),
                         bootstrapServers = s"localhost:$kafkaPort",
                         groupId = groupId,
                         autoOffsetReset = OffsetResetStrategy.EARLIEST)
    )
  }

  private def createProducer(kafkaPort: Int) = {
    KafkaProducer(
      KafkaProducer.Conf(new StringSerializer(),
                         new StringSerializer(),
                         bootstrapServers = s"localhost:$kafkaPort",
                         acks = "all"))
  }


  def createTopics(topicName: String*): Unit = {
    val adminClient = createAdmin(kafkaServer.kafkaPort)
    val topics = topicName.map(new NewTopic(_, 1, 1))
    val createTopicsResult = adminClient.createTopics(topics.toList.asJava)
    // finish futures
    topicName.foreach(t => createTopicsResult.values.get(t).get())
    adminClient.close()
  }

  private def createAdmin(kafkaPort: Int) = {
    val configMap = Map[String, AnyRef](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:$kafkaPort",
      AdminClientConfig.CLIENT_ID_CONFIG -> "admin",
      )
    AdminClient.create(configMap.asJava)
  }

}