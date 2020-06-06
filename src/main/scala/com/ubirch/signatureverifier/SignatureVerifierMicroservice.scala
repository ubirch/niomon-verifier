package com.ubirch.signatureverifier

import java.security.SignatureException
import java.util.UUID

import com.ubirch.client.protocol.MultiKeyProtocolVerifier
import com.ubirch.kafka.{RichAnyConsumerRecord, RichAnyProducerRecord}
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.bouncycastle.util.encoders.Hex

/**
 *
 * +=========+======+==================+======+=========+-------------+
 * | VERSION | UUID | [PREV-SIGNATURE] | TYPE | PAYLOAD | [SIGNATURE] |
 * +=========+======+==================+======+=========+-------------+
 * =   ➔ data used for signature (4 elements)
 * []  ➔ optional fields, depending on lower 4 bit of version
 *
 *
 * @param verifierFactory
 * @param runtime
 */
class SignatureVerifierMicroservice(verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier,
                                    runtime: NioMicroservice[Array[Byte], Array[Byte]]) extends NioMicroserviceLogic(runtime) {

  private val HARDWARE_ID_HEADER_KEY = "x-ubirch-hardware-id"
  val verifier: MultiKeyProtocolVerifier = verifierFactory(context)

  override def processRecord(record: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, Array[Byte]] = {

    try {
      record.findHeader(HARDWARE_ID_HEADER_KEY) match {

        case Some(hardwareIdHeader: String) =>

          val hardwareId = UUID.fromString(hardwareIdHeader)
          val msgPack = record.value()
          //Todo: Should I check the length of the package before splitting it?
          val signatureIdentifierLength = differentiateUbirchMsgPackVersion(msgPack)
          val restOfMessage = msgPack.dropRight(64 + signatureIdentifierLength)
          val signature = msgPack.takeRight(64)

          //Todo: Use cached KeyServiceClient
          verifier.verifyMulti(hardwareId, restOfMessage, 0, restOfMessage.length, signature) match {
            case Some(key) =>
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

  private def differentiateUbirchMsgPackVersion(msgPack: Array[Byte]) = {
    val hexMsgPack = Hex.toHexString(msgPack)
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

}

object SignatureVerifierMicroservice {
  def apply(verifierFactory: NioMicroservice.Context => MultiKeyProtocolVerifier)
           (runtime: NioMicroservice[Array[Byte], Array[Byte]]): SignatureVerifierMicroservice =
    new SignatureVerifierMicroservice(verifierFactory, runtime)
}
