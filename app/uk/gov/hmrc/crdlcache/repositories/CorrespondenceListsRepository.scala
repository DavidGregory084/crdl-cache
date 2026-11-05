/*
 * Copyright 2025 HM Revenue & Customs
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

package uk.gov.hmrc.crdlcache.repositories

import org.mongodb.scala.*
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.*
import org.mongodb.scala.model.Filters.*
import play.api.Logging
import play.api.libs.json.*
import uk.gov.hmrc.crdlcache.models
import uk.gov.hmrc.crdlcache.models.CodeListCode.code
import uk.gov.hmrc.crdlcache.models.CorrespondenceListInstruction.*
import uk.gov.hmrc.crdlcache.models.{
  CodeListCode,
  CodeListEntry,
  CorrespondenceListInstruction,
  CorrespondenceListKey
}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.play.json.Codecs

import java.time.Instant
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class CorrespondenceListsRepository @Inject() (mongoComponent: MongoComponent)(using
  ec: ExecutionContext
) extends CodeListsRepository[CorrespondenceListKey, CorrespondenceListInstruction](
    mongoComponent,
    collectionName = "correspondenceLists",
    extraCodecs =
      Codecs.playFormatSumCodecs[JsValue](Format(Reads.JsValueReads, Writes.jsValueWrites)) ++
        Codecs.playFormatSumCodecs[JsBoolean](Format(Reads.JsBooleanReads, Writes.jsValueWrites)),
    indexes = Seq(
      IndexModel(
        Indexes.ascending("domain", "phase", "codeListCode", "key", "value", "activeFrom"),
        IndexOptions().unique(true)
      )
    )
  )
  with Logging {

  override def activationDate(instruction: CorrespondenceListInstruction): Instant =
    instruction.activeFrom

  override def keyOfEntry(codeListEntry: CodeListEntry): CorrespondenceListKey =
    CorrespondenceListKey(
      codeListEntry.key,
      codeListEntry.value,
      codeListEntry.phase,
      codeListEntry.domain
    )

  override def filtersForKey(codeListCode: CodeListCode, key: CorrespondenceListKey): Seq[Bson] =
    Seq(
      key.domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
      key.phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
      equal("codeListCode", codeListCode.code),
      equal("key", key.key),
      equal("value", key.value)
    )

  def executeInstruction(
    session: ClientSession,
    instruction: CorrespondenceListInstruction
  ): Future[Unit] =
    instruction match {
      case UpsertEntry(codeListEntry) =>
        logger.debug(
          s"Upserting entry of correspondence list ${codeListEntry.codeListCode.code} with key ${codeListEntry.key}"
        )
        for {
          _ <- supersedePreviousEntries(
            session,
            codeListEntry.codeListCode,
            keyOfEntry(codeListEntry),
            codeListEntry.activeFrom,
            includeActiveFrom = false
          )
          _ <- upsertEntry(session, codeListEntry)
        } yield ()
      case InvalidateEntry(codeListEntry) =>
        logger.debug(
          s"Invalidating entry of correspondence list ${codeListEntry.codeListCode.code} with key ${codeListEntry.key}"
        )
        supersedePreviousEntries(
          session,
          codeListEntry.codeListCode,
          keyOfEntry(codeListEntry),
          codeListEntry.activeFrom,
          includeActiveFrom = true
        )
      case RecordMissingEntry(
            codeListCode,
            key,
            value,
            phase,
            domain,
            removedAt
          ) =>
        logger.debug(
          s"Recording removal of entry in correspondence list ${codeListCode.code} with key $key"
        )
        supersedePreviousEntries(
          session,
          codeListCode,
          CorrespondenceListKey(key, value, phase, domain),
          removedAt,
          includeActiveFrom = true
        )
    }
}
