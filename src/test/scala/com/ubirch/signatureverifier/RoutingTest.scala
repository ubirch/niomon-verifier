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

import java.util.{Base64, UUID}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka.{EnvelopeDeserializer, EnvelopeSerializer, MessageEnvelope}
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceMock}
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.{JSONProtocolDecoder, MsgPackProtocolDecoder}
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.JValue
import org.scalatest.{FlatSpec, Matchers}

//noinspection TypeAnnotation
class RoutingTest extends FlatSpec with Matchers with StrictLogging {
  implicit val messageEnvelopeSerializer = EnvelopeSerializer
  implicit val messageEnvelopeDeserializer = EnvelopeDeserializer

  val keyServerClient = (c: NioMicroservice.Context) => new KeyServerClient(c) {
    val knownKey = "sSqQYFHxAogbu0h+6CZKoF2ND8xRIY8qR/Vizrmw0Gg="

    // no caching for the tests
    override lazy val getPublicKeysCached: UUID => List[JValue] = getPublicKeys

    override def getPublicKeys(uuid: UUID): List[JValue] = {
      import org.json4s.JsonDSL._

      uuid.toString match {
        case "6eac4d0b-16e6-4508-8c46-22e7451ea5a1" =>
          List("pubKeyInfo" -> ("algorithm" -> "ECC_ED25519") ~ ("pubKey" -> knownKey))
        case "ffff160c-6117-5b89-ac98-15aeb52655e0" =>
          List("pubKeyInfo" -> ("algorithm" -> "ecdsa-p256v1") ~ ("pubKey" -> "kvdvWQ7NOT+HLDcrFqP/UZWy4QVcjfmmkfyzAgg8bitaK/FbHUPeqEji0UmCSlyPk5+4mEaEiZAHnJKOyqUZxA=="))
        case "" =>
          Nil
        case _ => Nil
      }
    }
  }
  val microservice = NioMicroserviceMock(SignatureVerifierMicroservice(c => new Verifier(keyServerClient(c))))
  microservice.outputTopics = Map("valid" -> "valid")
  microservice.errorTopic = Some("invalid")
  microservice.config = ConfigFactory.load().getConfig("signature-verifier")
  microservice.name = "signature-verifier"
  import microservice.kafkaMocks._


  "msgpack with valid signature" should "be routed to 'valid' queue" in {
    val message = Base64.getDecoder.decode("lRKwbqxNCxbmRQiMRiLnRR6loczvAdoAQFeKWyLOs+HQ0PiUfAmAEBM7RNOx0qs5h1j/7RFQe2B+03274Ab2RfDtD9vrG0i7UP1x2DI0DOAk1aDiHA68jg4=")
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

    val invalidTopicEnvelopes = consumeNumberStringMessagesFrom("invalid", 1, autoCommit = true)
    invalidTopicEnvelopes.size should be(1)

    val rejectedMessage = invalidTopicEnvelopes.head
    rejectedMessage.toString should equal("""{"error":"NullPointerException: null","causes":[],"microservice":"signature-verifier","requestId":"bar"}""")
  }

  "ecdsa verification" should "work" in {
    val message = Base64.getDecoder.decode("lSLEEP//FgxhF1uJrJgVrrUmVeAAxECUnW4kkga5FhldAMYFX7s8ZUTQwYZpV3ObvNKa27c+wVoGfmGN9zQwPbl2hXBq2femGe6NzSjUtQwAIVMXrERexEBKdNrNNjCpzGR/PwNNxxIwjFL++EEoSquEAyW/JW5cPblVnxC+rIgt4+0gUFbWy5IAZcOmmvtDFeP/u/G1lIU7")
    val pm = MsgPackProtocolDecoder.getDecoder.decode(message)
    val validMessage = MessageEnvelope(pm)
    logger.info(validMessage.toString)

    publishToKafka(new ProducerRecord("incoming", "foo", validMessage))

    val validTopicEnvelopes = consumeNumberMessagesFrom[MessageEnvelope]("valid", 1, autoCommit = true)
    validTopicEnvelopes.size should be(1)

    val approvedMessage = validTopicEnvelopes.head
    approvedMessage.toString should equal(validMessage.toString) // ProtocolMessage doesn't override equals :'(
  }
}