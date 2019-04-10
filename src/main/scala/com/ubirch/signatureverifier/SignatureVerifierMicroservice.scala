package com.ubirch.signatureverifier

import com.ubirch.kafka.MessageEnvelope
import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.{Failure, Success, Try}

class SignatureVerifierMicroservice(verifierFactory: NioMicroservice.Context => Verifier) extends NioMicroservice[MessageEnvelope, MessageEnvelope]("signature-verifier") {
  val verifier: Verifier = verifierFactory(context)
  private val httpStatusCodeKey = "http-status-code"

  override def processRecord(record: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {
    Try {
      val pm = record.value().ubirchPacket
      verifier.verify(pm.getUUID, pm.getSigned, 0, pm.getSigned.length, pm.getSignature)
    } match {
      case Success(true) => record.toProducerRecord(outputTopics("valid"))
      case Success(false) =>
        logger.warn(s"signature verification failed: $record, (Verifier.verify returned false)")
        record.withExtraHeaders(httpStatusCodeKey -> "400").toProducerRecord(outputTopics("invalid"))
      case Failure(e) =>
        logger.warn(s"signature verification failed: $record", e)
        record.withExtraHeaders(httpStatusCodeKey -> "400").toProducerRecord(outputTopics("invalid"))
    }
  }
}
