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

package com.ubirch

import akka.actor.ActorSystem
import akka.kafka._
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import com.ubirch.kafka._

import scala.concurrent.ExecutionContextExecutor

package object signatureverifier extends StrictLogging {

  val conf: Config = ConfigFactory.load
  implicit val system: ActorSystem = ActorSystem("signature-verifier")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val kafkaUrl: String = conf.getString("kafka.url")

  val producerConfig: Config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, MessageEnvelope] =
    ProducerSettings(producerConfig, new StringSerializer, EnvelopeSerializer)
      .withBootstrapServers(kafkaUrl)


  val consumerConfig: Config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings: ConsumerSettings[String, MessageEnvelope] =
    ConsumerSettings(consumerConfig, new StringDeserializer, EnvelopeDeserializer)
      .withBootstrapServers(kafkaUrl)
      .withGroupId("signature-verifier")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  val incomingTopic: String = conf.getString("kafka.topic.incoming")
  val validSignatureTopic: String = conf.getString("kafka.topic.outgoing.valid")
  val invalidSignatureTopic: String = conf.getString("kafka.topic.outgoing.invalid")

}

