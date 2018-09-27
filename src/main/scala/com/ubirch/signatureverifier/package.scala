package com.ubirch

import akka.actor.ActorSystem
import akka.kafka._
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafkasupport.MessageEnvelope
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContextExecutor

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

}

