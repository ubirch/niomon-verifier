package com.ubirch.signatureverifier

import com.typesafe.config.Config
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.{Failure, Success, Try}

class SignatureVerifierMicroservice(verifierFactory: Config => Verifier) extends NioMicroservice[MessageEnvelope, MessageEnvelope]("signature-verifier") {
  val verifier: Verifier = verifierFactory(config)

  override def processRecord(record: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {
    Try {
      val pm = record.value().ubirchPacket
      verifier.verify(pm.getUUID, pm.getSigned, 0, pm.getSigned.length, pm.getSignature)
    } match {
      case Success(true) => record.toProducerRecord(outputTopics("valid"))
      case Success(false) =>
        logger.warn(s"signature verification failed: $record, (Verifier.verify returned false)")
        record.toProducerRecord(outputTopics("invalid")).withExtraHeaders("http-status-code" -> "400")
      case Failure(e) =>
        logger.warn(s"signature verification failed: $record", e)
        record.toProducerRecord(outputTopics("invalid")).withExtraHeaders("http-status-code" -> "400")
    }
  }
}
