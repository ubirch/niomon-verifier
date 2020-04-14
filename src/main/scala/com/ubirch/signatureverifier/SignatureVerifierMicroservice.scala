package com.ubirch.signatureverifier

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.security.SignatureException
import java.util.Base64
import java.util.concurrent.TimeUnit

import com.ubirch.client.protocol.MultiKeyProtocolVerifier
import com.ubirch.kafka.{MessageEnvelope, _}
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import com.ubirch.protocol.ProtocolMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.msgpack.core.MessagePack
import org.redisson.api.RMapCache


class SignatureVerifierMicroservice(
                                     verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier,
                                     runtime: NioMicroservice[MessageEnvelope, MessageEnvelope]
                                   ) extends NioMicroserviceLogic(runtime) {

  import SignatureVerifierMicroservice._

  val verifier: MultiKeyProtocolVerifier = verifierFactory(context)
  // this cache is shared with verification-microservice (not a part of niomon) for faster verification on its side
  private val uppCache: RMapCache[Array[Byte], String] = context.redisCache.redisson.getMapCache("verifier-upp-cache")
  private val uppTtl = config.getDuration("verifier-upp-cache.timeToLive")
  private val uppMaxIdleTime = config.getDuration("verifier-upp-cache.maxIdleTime")

  override def processRecord(record: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {
    // try ... catch is here, because `verifier.verify` may also throw

    try {
      val pm = record.value().ubirchPacket
      verifier.verifyMulti(pm.getUUID, pm.getSigned, 0, pm.getSigned.length, pm.getSignature) match {
        case Some(key) =>
          // TODO: This won't work for payloads that are json objects, this just works for primitives
          val hash = pm.getPayload.asText().getBytes(StandardCharsets.UTF_8)
          uppCache.fastPut(hash, b64(rawPacket(pm)), uppTtl.toNanos, TimeUnit.NANOSECONDS, uppMaxIdleTime.toNanos, TimeUnit.NANOSECONDS)

          record.toProducerRecord(topic = onlyOutputTopic)
            .withExtraHeaders(("algorithm", key.getSignatureAlgorithm))

        case None => throw new SignatureException("Invalid signature")
      }
    } catch {
      case e: Exception =>
        throw WithHttpStatus(400, e)
    }
  }
}

object SignatureVerifierMicroservice {
  def apply(verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier)
           (runtime: NioMicroservice[MessageEnvelope, MessageEnvelope]): SignatureVerifierMicroservice =
    new SignatureVerifierMicroservice(verifierFactory, runtime)

  //// functions below this line are for formatting the upp in the way compatible with what verification-microservice is doing

  private val msgPackConfig = new MessagePack.PackerConfig().withStr8FormatSupport(false)

  private def rawPacket(upp: ProtocolMessage): Array[Byte] = {
    val out = new ByteArrayOutputStream(255)
    val packer = msgPackConfig.newPacker(out)

    packer.writePayload(upp.getSigned)
    packer.packBinaryHeader(upp.getSignature.length)
    packer.writePayload(upp.getSignature)
    packer.flush()
    packer.close()

    out.toByteArray
  }

  private def b64(x: Array[Byte]): String = if (x != null) Base64.getEncoder.encodeToString(x) else null
}
