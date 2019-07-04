package com.ubirch.signatureverifier

import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.client.keyservice.UbirchKeyService
import com.ubirch.crypto.PubKey
import com.ubirch.niomon.base.NioMicroservice

class CachingUbirchKeyService(context: NioMicroservice.Context) extends UbirchKeyService({
  val url = context.config.getString("ubirchKeyService.client.rest.host")
  if (url.startsWith("http://") || url.startsWith("https://")) url else s"http://$url"
}) with StrictLogging {
  lazy val getPublicKeysCached: UUID => Option[PubKey] =
    context.cached(super.getPublicKey _).buildCache("public-keys-cache", shouldCache = _.isDefined)

  override def getPublicKey(uuid: UUID): Option[PubKey] = getPublicKeysCached(uuid)
}
