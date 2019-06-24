/*
 * Copyright (c) 2019 ubirch GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ubirch.signatureverifier

import java.security.{InvalidKeyException, MessageDigest, NoSuchAlgorithmException, SignatureException}
import java.util.{Base64, UUID}

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.crypto.GeneratorKeyFactory
import com.ubirch.crypto.utils.Curve
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLive}
import com.ubirch.protocol.ProtocolVerifier
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JValue}
import skinny.http.HTTP

class KeyServerClient(context: NioMicroservice.Context) extends StrictLogging {
  implicit val formats: DefaultFormats = DefaultFormats
  val keyServerUrl: String = {
    val url = s"${context.config.getString("ubirchKeyService.client.rest.host")}/api/keyService/v1"
    if (url.startsWith("http://") || url.startsWith("https://")) url else s"http://$url"
  }

  lazy val getPublicKeysCached: UUID => List[JValue] =
    context.cached(getPublicKeys _).buildCache("public-keys-cache", shouldCache = _.nonEmpty)

  def getPublicKeys(uuid: UUID): List[JValue] = {
    val response = HTTP.get(keyServerUrl + "/pubkey/current/hardwareId/" + uuid.toString)
    logger.debug(s"received keys: {$uuid}: ${response.asString}")
    parse(response.asString).extract[List[JValue]]
  }
}

// TODO: this is also in the verification-microservice, extract this to a common lib (ubirch-crypto maybe?)
class Verifier(keyServer: KeyServerClient) extends ProtocolVerifier with StrictLogging {
  implicit val formats: DefaultFormats = DefaultFormats

  @throws[InvalidKeyException]
  @throws[NoSuchAlgorithmException]
  override def verify(uuid: UUID, data: Array[Byte], offset: Int, len: Int, signature: Array[Byte]): Boolean = {
    if (signature == null) throw new SignatureException("signature must not be null")
    logger.debug(s"VRFY: d=${Base64.getEncoder.encodeToString(data)}")
    logger.debug(s"VRFY: s=${Base64.getEncoder.encodeToString(signature)}")

    keyServer.getPublicKeysCached(uuid).headOption match {
      case Some(keyInfo) => val pubKeyBytes = Base64.getDecoder.decode((keyInfo \ "pubKeyInfo" \ "pubKey").extract[String])
        (keyInfo \ "pubKeyInfo" \ "algorithm").extract[String] match {
          case "ECC_ED25519" =>
            // Ed25519 uses SHA512 hashed messages
            val digest: MessageDigest = MessageDigest.getInstance("SHA-512")
            digest.update(data, offset, len)
            val dataToVerify = digest.digest

            logger.debug(s"verifying ED25519: ${Base64.getEncoder.encodeToString(dataToVerify)}")
            GeneratorKeyFactory.getPubKey(pubKeyBytes, Curve.Ed25519).verify(dataToVerify, signature)
          case a if a == "ECC_ECDSA" || a == "ecdsa-p256v1" =>
            val dataToVerify = new Array[Byte](len)
            System.arraycopy(data, offset, dataToVerify, 0, len)

            logger.debug(s"verifying ED25519: ${Base64.getEncoder.encodeToString(dataToVerify)}")
            GeneratorKeyFactory.getPubKey(pubKeyBytes, Curve.Ed25519).verify(dataToVerify, signature)
          case algorithm: String =>
            throw new NoSuchAlgorithmException(s"unsupported algorithm: $algorithm")
        }
      case None => throw new NoSuchElementException("public key not found")
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val _ = NioMicroserviceLive(
      "signature-verifier",
      SignatureVerifierMicroservice(c => new Verifier(new KeyServerClient(c)))
    ).runUntilDoneAndShutdownProcess
  }
}
