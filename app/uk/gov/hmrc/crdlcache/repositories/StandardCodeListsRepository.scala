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
import uk.gov.hmrc.crdlcache.models.Instruction.{InvalidateEntry, RecordMissingEntry, UpsertEntry}
import uk.gov.hmrc.crdlcache.models.{CodeListEntry, CodeListKey, Instruction}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.play.json.Codecs

import java.time.Instant
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class StandardCodeListsRepository @Inject() (mongoComponent: MongoComponent)(using
  ec: ExecutionContext
) extends CodeListsRepository[CodeListKey, Instruction](
    mongoComponent,
    collectionName = "codelists",
    extraCodecs =
      Codecs.playFormatSumCodecs[JsValue](Format(Reads.JsValueReads, Writes.jsValueWrites)) ++
        Codecs.playFormatSumCodecs[JsBoolean](Format(Reads.JsBooleanReads, Writes.jsValueWrites)),
    indexes = Seq(
      IndexModel(
        Indexes.ascending("codeListCode", "phase", "domain", "key", "activeFrom"),
        IndexOptions().unique(true)
      ),
      IndexModel(
        Indexes.ascending(
          "codeListCode",
          "properties.countableFlag",
          "key",
          "activeFrom"
        )
      )
    )
  )
  with Logging {

  override def activationDate(instruction: Instruction): Instant = instruction.activeFrom

  override def keyOfEntry(codeListEntry: CodeListEntry): CodeListKey =
    CodeListKey(codeListEntry.key, codeListEntry.phase, codeListEntry.domain)

  override def filtersForKey(key: CodeListKey): Seq[Bson] =
    Seq(equal("key", key.key))
      ++ key.phase.fold(None)(p => Seq(equal("phase", p)))
      ++ key.domain.fold(None)(d => Seq(equal("domain", d)))

  def executeInstruction(session: ClientSession, instruction: Instruction): Future[Unit] =
    instruction match {
      case UpsertEntry(codeListEntry) =>
        logger.debug(
          s"Upserting entry of codelist ${codeListEntry.codeListCode.code} with key ${codeListEntry.key}"
        )
        for {
          _ <- supersedePreviousEntries(
            session,
            codeListEntry.codeListCode,
            CodeListEntry.asCodeListKey(codeListEntry),
            codeListEntry.activeFrom,
            includeActiveFrom = false
          )
          _ <- upsertEntry(session, codeListEntry)
        } yield ()
      case InvalidateEntry(codeListEntry) =>
        logger.debug(
          s"Invalidating entry of codelist ${codeListEntry.codeListCode.code} with key ${codeListEntry.key}"
        )
        supersedePreviousEntries(
          session,
          codeListEntry.codeListCode,
          CodeListEntry.asCodeListKey(codeListEntry),
          codeListEntry.activeFrom,
          includeActiveFrom = true
        )
      case RecordMissingEntry(codeListCode, key, phase, domain, removedAt) =>
        logger.debug(s"Recording removal of entry in codelist ${codeListCode.code} with key $key")
        supersedePreviousEntries(
          session,
          codeListCode,
          CodeListKey(key, phase, domain),
          removedAt,
          includeActiveFrom = true
        )
    }
}
