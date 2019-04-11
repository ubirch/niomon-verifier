package com.ubirch.signatureverifier

import com.ubirch.kafka.{MessageEnvelope, _}
import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.{Failure, Try}

class SignatureVerifierMicroservice(verifierFactory: NioMicroservice.Context => Verifier) extends NioMicroservice[MessageEnvelope, MessageEnvelope]("signature-verifier") {
  val verifier: Verifier = verifierFactory(context)
  override def processRecord(record: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {
    Try {
      val pm = record.value().ubirchPacket
      verifier.verify(pm.getUUID, pm.getSigned, 0, pm.getSigned.length, pm.getSignature)
    }.filter(identity).recoverWith { case f: Throwable =>
      Failure(WithHttpStatus(400, f))
    }.get

    record.toProducerRecord(outputTopics("valid"))
  }
}
