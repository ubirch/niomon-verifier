package com.ubirch.signatureverifier

import java.security.SignatureException
import java.util.concurrent.TimeUnit
import java.util.{Base64, UUID}

import com.ubirch.client.protocol.MultiKeyProtocolVerifier
import com.ubirch.kafka.{RichAnyConsumerRecord, RichAnyProducerRecord}
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.redisson.api.RMapCache

class SignatureVerifierMicroserviceNew(verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier,
                                       runtime: NioMicroservice[Array[Byte], Array[Byte]]) extends NioMicroserviceLogic(runtime) {

  import SignatureVerifierMicroserviceNew._

  private val HARDWARE_ID_HEADER_KEY = "x-ubirch-hardware-id"
  val verifier: MultiKeyProtocolVerifier = verifierFactory(context)

  //   this cache is shared with verification-microservice (not a part of niomon) for faster verification on its side
  private val uppCache: RMapCache[Array[Byte], String] = context.redisCache.redisson.getMapCache("verifier-upp-cache")
  private val uppTtl = config.getDuration("verifier-upp-cache.timeToLive")
  private val uppMaxIdleTime = config.getDuration("verifier-upp-cache.maxIdleTime")

  override def processRecord(record: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, Array[Byte]] = {

    try {
      record.findHeader(HARDWARE_ID_HEADER_KEY) match {

        case Some(hardwareIdHeader: String) =>

          val hardwareId = UUID.fromString(hardwareIdHeader)
          val msgPack = record.value()
          //Todo: Should I check the length of the package before splitting it?
          val signatureIdentifierLength = differentiateUbirchMsgPackVersion(msgPack, hardwareId)
          val payload = msgPack.dropRight(64 + signatureIdentifierLength)
          val signature = msgPack.takeRight(64)

          //Todo: Use cached KeyServiceClient
          verifier.verifyMulti(hardwareId, payload, 0, payload.length, signature) match {

            case Some(key) =>
              //Todo: Is equivalent? val hash = pm.getPayload.asText().getBytes(StandardCharsets.UTF_8)
              val hash = payload
              uppCache.fastPut(hash, b64(msgPack), uppTtl.toNanos, TimeUnit.NANOSECONDS, uppMaxIdleTime.toNanos, TimeUnit.NANOSECONDS)

              record.toProducerRecord[Array[Byte]](topic = onlyOutputTopic)
                .withExtraHeaders(("algorithm", key.getSignatureAlgorithm))

            case None =>
              val errorMsg = s"signature verification failed for msgPack of hardwareId $hardwareId."
              logger.error(errorMsg)
              throw new SignatureException("Invalid signature")
          }
        case None =>
          val errorMsg = s"Header with key $HARDWARE_ID_HEADER_KEY is missing. Cannot verify msgPack."
          logger.error(errorMsg)
          throw new SignatureException(errorMsg)
      }
    } catch {
      case e: Exception =>
        throw WithHttpStatus(400, e)
    }
  }

  private def differentiateUbirchMsgPackVersion(msgPack: Array[Byte], hardwareId: UUID) = {
    val hexMsgPack = convertBytesToHex(msgPack, hardwareId)
    hexMsgPack(2) match {
      case '1' =>
        logger.info("msgPack version 1 was found")
        3
      case '2' =>
        logger.info("msgPack version 2 was found")
        2
      case 'c' if hexMsgPack.slice(176, 178) == "54" =>
        logger.info("trackle msgPack was found")
        3
      case thirdLetter =>
        val errorMsg = s"Couldn't identify Ubirch msgPack protocol as third letter is neither 1, 2 or 'c' but $thirdLetter"
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
    }
  }

  private def convertBytesToHex(bytes: Seq[Byte], hardwareId: UUID): String = {
    try {
      val sb = new StringBuilder
      for (b <- bytes) {
        sb.append(String.format("%02x", Byte.box(b)))
      }
      sb.toString
    } catch {
      case ex: Throwable =>
        val errorMsg = s"couldn't convert bytes for $hardwareId to hexString"
        throw logAndThrowSignExc(ex, errorMsg)
    }
  }

  private def logAndThrowSignExc(ex: Throwable, errorMsg: String): SignatureException = {
    logger.error(errorMsg, ex)
    new SignatureException(errorMsg)
  }

}

object SignatureVerifierMicroserviceNew {
  def apply(verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier)
           (runtime: NioMicroservice[Array[Byte], Array[Byte]]): SignatureVerifierMicroserviceNew =
    new SignatureVerifierMicroserviceNew(verifierFactory, runtime)

  //// functions below this line are for formatting the upp in the way compatible with what verification-microservice is doing

  //Todo: Is this also per default this way?
  //    private val msgPackConfig = new MessagePack.PackerConfig().withStr8FormatSupport(false)


  private def b64(x: Array[Byte]): String = if (x != null) Base64.getEncoder.encodeToString(x) else null

}
