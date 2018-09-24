package com.ubirch

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.typesafe.config.{Config, ConfigFactory}
import com.ubirch.kafkasupport.MessageEnvelope
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContextExecutor

final case class MessageEnvelopeWithRouting[T](messageEnvelope: MessageEnvelope[T], destinationTopic: String)

package object signatureverifier {

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

  /**
    * ToDo BjB 21.09.18 : actual validation should happen somewhere below
    */
  def determineRoutingBasedOnSignature(envelope: MessageEnvelope[String]): MessageEnvelopeWithRouting[String] = {
    if (signatureIsValid(envelope.payload)){
      MessageEnvelopeWithRouting(envelope, validSignatureTopic)
    } else {
      MessageEnvelopeWithRouting(envelope, invalidSignatureTopic)
    }
  }


  // ToDo BjB 21.09.18 :changing this should also be reflected in com.ubirch.signatureverification.RoutingTest
  private def signatureIsValid(payload: String): Boolean = {
    !payload.contains("invalid")
  }
}
