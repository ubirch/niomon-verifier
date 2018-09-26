package com.ubirch

import java.security.{InvalidKeyException, KeyPair, MessageDigest, SignatureException}
import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafkasupport.MessageEnvelope
import com.ubirch.protocol.ProtocolVerifier
import com.ubirch.protocol.codec.JSONProtocolDecoder
import net.i2p.crypto.eddsa.spec.{EdDSANamedCurveSpec, EdDSANamedCurveTable, EdDSAPrivateKeySpec, EdDSAPublicKeySpec}
import net.i2p.crypto.eddsa.{EdDSAEngine, EdDSAPrivateKey, EdDSAPublicKey}
import org.apache.commons.codec.binary.Hex
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

package object signatureverifier extends StrictLogging {

  final case class MessageEnvelopeWithRouting[T](messageEnvelope: MessageEnvelope[T], destinationTopic: String)

  val conf: Config = ConfigFactory.load
  implicit val system: ActorSystem = ActorSystem("signature-verifier")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val kafkaUrl: String = conf.getString("kafka.url")

  val producerConfig: Config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(kafkaUrl)


  val consumerConfig: Config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(consumerConfig, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaUrl)
      .withGroupId("signature-verifier")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  val incomingTopic: String = conf.getString("kafka.topic.incoming")
  val validSignatureTopic: String = conf.getString("kafka.topic.outgoing.valid")
  val invalidSignatureTopic: String = conf.getString("kafka.topic.outgoing.invalid")


  val verifierStream: RunnableGraph[DrainingControl[Done]] =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(incomingTopic))
      .map { msg =>
        val messageEnvelope = MessageEnvelope.fromRecord(msg.record)
        val envelopeWithRouting = determineRoutingBasedOnSignature(messageEnvelope)

        val recordToSend = MessageEnvelope.toRecord(envelopeWithRouting.destinationTopic, msg.record.key(), envelopeWithRouting.messageEnvelope)
        ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
          recordToSend,
          msg.committableOffset
        )
      }
      .toMat(Producer.commitableSink(producerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)

  val verifier = new Verifier

  /**
    * ToDo BjB 21.09.18 : actual validation should happen somewhere below
    */
  def determineRoutingBasedOnSignature(envelope: MessageEnvelope[String]): MessageEnvelopeWithRouting[String] = {
    Try(JSONProtocolDecoder.getDecoder.decode(envelope.payload, verifier)) match {
      case Success(pm) => MessageEnvelopeWithRouting(envelope, validSignatureTopic)
      case Failure(e) =>
        logger.error(s"signature verification failed: $envelope", e)
        MessageEnvelopeWithRouting(envelope, invalidSignatureTopic)
    }
  }

  class Verifier extends ProtocolVerifier with StrictLogging {
    val spec: EdDSANamedCurveSpec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.CURVE_ED25519_SHA512)


    @throws[SignatureException]
    @throws[InvalidKeyException]
    override def verify(uuid: UUID, data: Array[Byte], offset: Int, len: Int, signature: Array[Byte]): Boolean = {
      val digest: MessageDigest = MessageDigest.getInstance("SHA-512")
      val signEngine = new EdDSAEngine(digest)

      val priv: Array[Byte] = Hex.decodeHex("a6abdc5466e0ab864285ba925452d02866638a8acb5ebdc065d2506661301417".toCharArray)
      val pub: Array[Byte] = Hex.decodeHex("b12a906051f102881bbb487ee8264aa05d8d0fcc51218f2a47f562ceb9b0d068".toCharArray)

      val keypair = new KeyPair(
        new EdDSAPublicKey(new EdDSAPublicKeySpec(pub, spec)),
        new EdDSAPrivateKey(new EdDSAPrivateKeySpec(priv, spec)))

      // create hash of message
      digest.update(data, offset, len)
      val dataToVerify = digest.digest

      // verify signature using the hash
      signEngine.initVerify(keypair.getPublic)
      signEngine.update(dataToVerify, 0, dataToVerify.length)
      logger.debug(s"VRFY: (${signature.length}) ${Hex.encodeHexString(signature)}")
      signEngine.verify(signature)
    }
  }

}

