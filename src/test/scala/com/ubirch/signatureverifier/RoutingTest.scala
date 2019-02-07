/*
 * Copyright (c) 2019 ubirch GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ubirch.signatureverifier

import java.time.Duration
import java.util.UUID

import akka.stream.UniqueKillSwitch
import cakesolutions.kafka.testkit.KafkaServer
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka.{EnvelopeDeserializer, EnvelopeSerializer, MessageEnvelope}
import com.ubirch.protocol.{ProtocolMessage, ProtocolMessageViews}
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.apache.commons.codec.binary.Hex
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetResetStrategy}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._

//noinspection TypeAnnotation
class RoutingTest extends FlatSpec with Matchers with BeforeAndAfterAll with StrictLogging {
  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
  mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
  mapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false)
  mapper.setConfig(mapper.getSerializationConfig.withView(classOf[ProtocolMessageViews.WithSignedData]))

  "msgpack with valid signature" should "be routed to 'valid' queue" in {
    val message = Hex.decodeHex("9512b06eac4d0b16e645088c4622e7451ea5a1ccef01da0040578a5b22ceb3e1d0d0f8947c098010133b44d3b1d2ab398758ffed11507b607ed37dbbe006f645f0ed0fdbeb1b48bb50fd71d832340ce024d5a0e21c0ebc8e0e".toCharArray)
    val pm = MsgPackProtocolDecoder.getDecoder.decode(message)
    val validMessage = MessageEnvelope(pm)
    logger.info(validMessage.toString)

    producer.send(new ProducerRecord("incoming", "foo", validMessage))
    validTopicConsumer.subscribe(List("valid").asJava)

    val validTopicRecords: ConsumerRecords[String, MessageEnvelope] = validTopicConsumer.poll(Duration.ofSeconds(10))
    validTopicRecords.count() should be(1)

    val approvedMessage = validTopicRecords.iterator().next()
    approvedMessage.value().toString should equal(validMessage.toString) // ProtocolMessage doesn't override equals :'(
  }

  "json with valid signature" should "be routed to 'valid' queue" in {
    val message = "{\"version\":18,\"uuid\":\"6eac4d0b-16e6-4508-8c46-22e7451ea5a1\",\"hint\":239,\"signature\":\"YyC6ChlzkEOxL0oH98ytZz4ZOUEmE3uFlt3Ildy2X1/Pdp9BtSQvMScZKjUK6Y0berKHKR7LRYAwD7Ko+BBXCA==\",\"payload\":1}"
    val pm = JSONProtocolDecoder.getDecoder.decode(message)
    val validMessage = MessageEnvelope(pm)
    logger.info(validMessage.toString)

    producer.send(new ProducerRecord("incoming", "foo", validMessage))
    validTopicConsumer.subscribe(List("valid").asJava)

    val validTopicRecords: ConsumerRecords[String, MessageEnvelope] = validTopicConsumer.poll(Duration.ofSeconds(10))
    validTopicRecords.count() should be(1)

    val approvedMessage = validTopicRecords.iterator().next()
    approvedMessage.value().toString should equal(validMessage.toString) // ProtocolMessage doesn't override equals :'(
  }

  "json with invalid signature " should "be routed to 'invalid' queue" in {
    val invalidMessage = MessageEnvelope(new ProtocolMessage())
    producer.send(new ProducerRecord("incoming", "bar", invalidMessage))
    invalidTopicConsumer.subscribe(List("invalid").asJava)

    val invalidTopicRecords: ConsumerRecords[String, MessageEnvelope] = invalidTopicConsumer.poll(Duration.ofSeconds(10))
    invalidTopicRecords.count() should be(1)
    val rejectedMessage = invalidTopicRecords.iterator().next()
    rejectedMessage.value().toString should equal(invalidMessage.toString) // ProtocolMessage doesn't override equals :'(
  }

  val kafkaServer = new KafkaServer(9992)
  val producer = createProducer(kafkaServer.kafkaPort)
  val invalidTopicConsumer = createConsumer(kafkaServer.kafkaPort, "1")
  val validTopicConsumer = createConsumer(kafkaServer.kafkaPort, "2")
  var stream: UniqueKillSwitch = _

  override def beforeAll(): Unit = {
    kafkaServer.startup()
    createTopics("incoming", "invalid", "valid")
    val keyServerClient = new KeyServerClient("") {
      val knowUUID = UUID.fromString("6eac4d0b-16e6-4508-8c46-22e7451ea5a1")

      override def getPublicKeys(uuid: UUID): List[Array[Byte]] = {
        if (uuid == uuid) {
          List(Hex.decodeHex("b12a906051f102881bbb487ee8264aa05d8d0fcc51218f2a47f562ceb9b0d068".toCharArray))
        } else {
          Nil
        }
      }
    }
    stream = SignatureVerifier(new Verifier(keyServerClient)).run()
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

  override def afterAll(): Unit = {
    stream.shutdown()
    producer.close()
    invalidTopicConsumer.close()
    validTopicConsumer.close()
    kafkaServer.close()
  }

  private def createConsumer(kafkaPort: Int, groupId: String) = {
    KafkaConsumer(
      KafkaConsumer.Conf(new StringDeserializer(),
        EnvelopeDeserializer,
        bootstrapServers = s"localhost:$kafkaPort",
        groupId = groupId,
        autoOffsetReset = OffsetResetStrategy.EARLIEST)
    )
  }

  private def createProducer(kafkaPort: Int) = {
    KafkaProducer(
      KafkaProducer.Conf(new StringSerializer(),
        EnvelopeSerializer,
        bootstrapServers = s"localhost:$kafkaPort",
        acks = "all"))
  }

}