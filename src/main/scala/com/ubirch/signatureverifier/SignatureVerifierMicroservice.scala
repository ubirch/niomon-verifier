package com.ubirch.signatureverifier

import java.security.SignatureException

import com.ubirch.kafka.{MessageEnvelope, _}
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.protocol.ProtocolVerifier
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

class SignatureVerifierMicroservice(
  verifierFactory: NioMicroservice.Context => ProtocolVerifier,
  runtime: NioMicroservice[MessageEnvelope, MessageEnvelope]
) extends NioMicroserviceLogic(runtime) {
  val verifier: ProtocolVerifier = verifierFactory(context)
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

object SignatureVerifierMicroservice {
  def apply(verifierFactory: NioMicroservice.Context => ProtocolVerifier)
    (runtime: NioMicroservice[MessageEnvelope, MessageEnvelope]): SignatureVerifierMicroservice =
    new SignatureVerifierMicroservice(verifierFactory, runtime)
}
