package com.ubirch.signatureverifier

import java.security.SignatureException

import com.ubirch.kafka.{MessageEnvelope, _}
import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

class SignatureVerifierMicroservice(verifierFactory: NioMicroservice.Context => Verifier) extends NioMicroservice[MessageEnvelope, MessageEnvelope]("signature-verifier") {
  val verifier: Verifier = verifierFactory(context)
  override def processRecord(record: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {
    // try ... catch is here, because `verifier.verify` may also throw
    try {
      val pm = record.value().ubirchPacket
      if (!verifier.verify(pm.getUUID, pm.getSigned, 0, pm.getSigned.length, pm.getSignature)) {
        throw new SignatureException("Invalid signature")
      }
    } catch { case e: Exception =>
      throw WithHttpStatus(400, e)
    }

    record.toProducerRecord(onlyOutputTopic)
  }
}
