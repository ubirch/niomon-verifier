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

import java.util.UUID

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka.{EnvelopeDeserializer, EnvelopeSerializer, MessageEnvelope}
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.commons.codec.binary.Hex
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

//noinspection TypeAnnotation
class RoutingTest extends FlatSpec with Matchers with BeforeAndAfterAll with StrictLogging with EmbeddedKafka {
  implicit val messageEnvelopeSerializer = EnvelopeSerializer
  implicit val messageEnvelopeDeserializer = EnvelopeDeserializer

  "msgpack with valid signature" should "be routed to 'valid' queue" in {
    val message = Hex.decodeHex("9512b06eac4d0b16e645088c4622e7451ea5a1ccef01da0040578a5b22ceb3e1d0d0f8947c098010133b44d3b1d2ab398758ffed11507b607ed37dbbe006f645f0ed0fdbeb1b48bb50fd71d832340ce024d5a0e21c0ebc8e0e".toCharArray)
    val pm = MsgPackProtocolDecoder.getDecoder.decode(message)
    val validMessage = MessageEnvelope(pm)
    logger.info(validMessage.toString)

    publishToKafka(new ProducerRecord("incoming", "foo", validMessage))

    val validTopicEnvelopes = consumeNumberMessagesFrom[MessageEnvelope]("valid", 1, autoCommit = true)
    validTopicEnvelopes.size should be(1)

    val approvedMessage = validTopicEnvelopes.head
    approvedMessage.toString should equal(validMessage.toString) // ProtocolMessage doesn't override equals :'(
  }

  "json with valid signature" should "be routed to 'valid' queue" in {
    val message = "{\"version\":18,\"uuid\":\"6eac4d0b-16e6-4508-8c46-22e7451ea5a1\",\"hint\":239,\"signature\":\"YyC6ChlzkEOxL0oH98ytZz4ZOUEmE3uFlt3Ildy2X1/Pdp9BtSQvMScZKjUK6Y0berKHKR7LRYAwD7Ko+BBXCA==\",\"payload\":1}"
    val pm = JSONProtocolDecoder.getDecoder.decode(message)
    val validMessage = MessageEnvelope(pm)
    logger.info(validMessage.toString)

    publishToKafka(new ProducerRecord("incoming", "foo", validMessage))

    val validTopicEnvelopes = consumeNumberMessagesFrom[MessageEnvelope]("valid", 1, autoCommit = true)
    validTopicEnvelopes.size should be(1)

    val approvedMessage = validTopicEnvelopes.head
    approvedMessage.toString should equal(validMessage.toString) // ProtocolMessage doesn't override equals :'(
  }

  "json with invalid signature " should "be routed to 'invalid' queue" in {
    val invalidMessage = MessageEnvelope(new ProtocolMessage())
    publishToKafka(new ProducerRecord("incoming", "bar", invalidMessage))

    val invalidTopicEnvelopes = consumeNumberMessagesFrom[MessageEnvelope]("invalid", 1, autoCommit = true)
    invalidTopicEnvelopes.size should be(1)

    val rejectedMessage = invalidTopicEnvelopes.head
    rejectedMessage.toString should equal(invalidMessage.toString) // ProtocolMessage doesn't override equals :'(
  }

  var microservice: SignatureVerifierMicroservice = _
  var control: DrainingControl[Done] = _

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
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
    microservice = new SignatureVerifierMicroservice(_ => new Verifier(keyServerClient))
    control = microservice.run
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    control.drainAndShutdown()(microservice.system.dispatcher)
  }
}