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

import akka.NotUsed
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Keep, RestartSink, RestartSource, RunnableGraph, Sink, Source}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.DefaultFormats

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Verify signatures on ubirch protocol messages.
  *
  * @author Matthias L. Jugel
  */
object SignatureVerifier extends StrictLogging {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val kafkaSource: Source[ConsumerMessage.CommittableMessage[String, MessageEnvelope], NotUsed] =
    RestartSource.withBackoff(
      minBackoff = 2.seconds,
      maxBackoff = 1.minute,
      randomFactor = 0.2
    ) { () => Consumer.committableSource(consumerSettings, Subscriptions.topics(incomingTopic)) }

  val kafkaSink: Sink[ProducerMessage.Envelope[String, MessageEnvelope, ConsumerMessage.Committable], NotUsed] =
    RestartSink.withBackoff(
      minBackoff = 2.seconds,
      maxBackoff = 1.minute,
      randomFactor = 0.2
    ) { () => Producer.commitableSink(producerSettings) }

  def apply(verifier: Verifier): RunnableGraph[UniqueKillSwitch] = {
    kafkaSource
      .viaMat(KillSwitches.single)(Keep.right)
      .map { msg =>
        val record = msg.record
        val routedRecord = determineRoutingBasedOnSignature(record, verifier)

        ProducerMessage.Message[String, MessageEnvelope, ConsumerMessage.CommittableOffset](
          routedRecord,
          msg.committableOffset
        )
      }
      .to(kafkaSink)
  }

  def determineRoutingBasedOnSignature(record: ConsumerRecord[String, MessageEnvelope], verifier: Verifier): ProducerRecord[String, MessageEnvelope] = {
    Try {
      val pm = record.value().ubirchPacket
      verifier.verify(pm.getUUID, pm.getSigned, 0, pm.getSigned.length, pm.getSignature)
    } match {
      case Success(true) => record.toProducerRecord(validSignatureTopic)
      case Success(false) =>
        logger.warn(s"signature verification failed: $record, (Verifier.verify returned false)")
        record.toProducerRecord(invalidSignatureTopic).withExtraHeaders("http-status-code" -> "400")
      case Failure(e) =>
        logger.warn(s"signature verification failed: $record", e)
        record.toProducerRecord(invalidSignatureTopic).withExtraHeaders("http-status-code" -> "400")
    }
  }
}
