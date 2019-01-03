package com.ubirch.signatureverifier

import java.security.{InvalidKeyException, MessageDigest, SignatureException}
import java.util.{Base64, UUID}

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.protocol.ProtocolVerifier
import net.i2p.crypto.eddsa.spec.{EdDSANamedCurveSpec, EdDSANamedCurveTable, EdDSAPublicKeySpec}
import net.i2p.crypto.eddsa.{EdDSAEngine, EdDSAPublicKey}
import org.apache.commons.codec.binary.Hex
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JValue}
import skinny.http.HTTP

object Main {

  val KEY_SERVER_URL: String = {
    val url = s"${conf.getString("ubirchKeyService.client.rest.host")}/api/keyService/v1"
    if (url.startsWith("http://") || url.startsWith("https://")) url else s"http://$url"
  }

  def main(args: Array[String]) {
    SignatureVerifier(new Verifier(new KeyServerClient(KEY_SERVER_URL))).run()
  }
}

class KeyServerClient(keyServerUrl: String) extends StrictLogging {
  implicit val formats: DefaultFormats = DefaultFormats

  def getPublicKeys(uuid: UUID): List[Array[Byte]] = {
    val response = HTTP.get(keyServerUrl + "/pubkey/current/hardwareId/" + uuid.toString)
    logger.info(response.asString)
    val keys = parse(response.asString).extract[List[JValue]]
    keys.map(key => Base64.getDecoder.decode((key \ "pubKeyInfo" \ "pubKey").extract[String]))
  }
}

class Verifier(keyServer: KeyServerClient) extends ProtocolVerifier with StrictLogging {
  val spec: EdDSANamedCurveSpec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.CURVE_ED25519_SHA512)

  @throws[SignatureException]
  @throws[InvalidKeyException]
  override def verify(uuid: UUID, data: Array[Byte], offset: Int, len: Int, signature: Array[Byte]): Boolean = {
    if (signature == null) throw new SignatureException("signature must not be null")

    val digest: MessageDigest = MessageDigest.getInstance("SHA-512")
    val signEngine = new EdDSAEngine(digest)

    keyServer.getPublicKeys(uuid).map { pubKeyBytes =>
      val publicKey = new EdDSAPublicKey(new EdDSAPublicKeySpec(pubKeyBytes, spec))

      // create hash of message
      digest.update(data, offset, len)
      val dataToVerify = digest.digest

      // verify signature using the hash
      signEngine.initVerify(publicKey)
      signEngine.update(dataToVerify, 0, dataToVerify.length)
      logger.debug(s"VRFY: (${signature.length}) ${Hex.encodeHexString(signature)}")
      signEngine.verify(signature)
    }.headOption.getOrElse(false)
  }
}