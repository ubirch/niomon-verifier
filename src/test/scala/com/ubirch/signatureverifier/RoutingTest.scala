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

//noinspection TypeAnnotation
class RoutingTest extends FunSuite with Matchers with BeforeAndAfterAll {

  test("valid signature is routed to 'valid' queue") {
    val validMessage = "{\"version\":18,\"uuid\":\"6eac4d0b-16e6-4508-8c46-22e7451ea5a1\",\"hint\":239,\"signature\":\"YyC6ChlzkEOxL0oH98ytZz4ZOUEmE3uFlt3Ildy2X1/Pdp9BtSQvMScZKjUK6Y0berKHKR7LRYAwD7Ko+BBXCA==\",\"payload\":1}"
    producer.send(MessageEnvelope.toRecord("incoming", "foo", MessageEnvelope(validMessage)))
    validTopicConsumer.subscribe(List("valid").asJava)

    val validTopicRecords: ConsumerRecords[String, String] = validTopicConsumer.poll(5000)
    validTopicRecords.count() should be(1)

    val approvedMessage = MessageEnvelope.fromRecord(validTopicRecords.iterator().next())
    approvedMessage.payload should equal(validMessage)
  }

  test("invalid signature is routed to 'invalid' queue") {
    producer.send(MessageEnvelope.toRecord("incoming", "bar", MessageEnvelope("invalid signature")))
    invalidTopicConsumer.subscribe(List("invalid").asJava)

    val invalidTopicRecords: ConsumerRecords[String, String] = invalidTopicConsumer.poll(5000)
    invalidTopicRecords.count() should be(1)
    val rejectedMessage = MessageEnvelope.fromRecord(invalidTopicRecords.iterator().next())
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