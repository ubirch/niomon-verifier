package com.ubirch.signatureverifier

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafkasupport.MessageEnvelope
import com.ubirch.protocol.codec.JSONProtocolDecoder

import scala.util.{Failure, Success, Try}

/**
  * Add description.
  *
  * @author Matthias L. Jugel
  */
object SignatureVerifier extends StrictLogging {
  def apply(verifier: Verifier): RunnableGraph[Consumer.DrainingControl[Done]] = {
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(incomingTopic))
      .map { msg =>
        val messageEnvelope = MessageEnvelope.fromRecord(msg.record)
        val envelopeWithRouting = determineRoutingBasedOnSignature(messageEnvelope, verifier)

        val recordToSend = MessageEnvelope.toRecord(envelopeWithRouting.destinationTopic, msg.record.key(), envelopeWithRouting.messageEnvelope)
        ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
          recordToSend,
          msg.committableOffset
        )
      }
      .toMat(Producer.commitableSink(producerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
  }

  def determineRoutingBasedOnSignature(envelope: MessageEnvelope[String], verifier: Verifier): MessageEnvelopeWithRouting[String] = {
    Try(JSONProtocolDecoder.getDecoder.decode(envelope.payload, verifier)) match {
      case Success(pm) => MessageEnvelopeWithRouting(envelope, validSignatureTopic)
      case Failure(e) =>
        logger.error(s"signature verification failed: $envelope", e)
        MessageEnvelopeWithRouting(envelope, invalidSignatureTopic)
    }
  }
}
