package com.ubirch.signatureverifier

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafkasupport.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import org.json4s.DefaultFormats

import scala.util.{Failure, Success, Try}

/**
  * Verify signatures on ubirch protocol messages.
  *
  * @author Matthias L. Jugel
  */
object SignatureVerifier extends StrictLogging {
  implicit val formats: DefaultFormats.type = DefaultFormats
  import org.json4s.jackson.JsonMethods._

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
    Try {
      val pm = mapper.readValue(envelope.payload, classOf[ProtocolMessage])
      verifier.verify(pm.getUUID, pm.getSigned, 0, pm.getSigned.length, pm.getSignature)
    } match {
      case Success(pm) => MessageEnvelopeWithRouting(envelope, validSignatureTopic)
      case Failure(e) =>
        logger.warn(s"signature verification failed: $envelope", e)
        MessageEnvelopeWithRouting(envelope, invalidSignatureTopic)
    }
  }
}
