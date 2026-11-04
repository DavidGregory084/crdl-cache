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
import uk.gov.hmrc.crdlcache.models.errors.MongoError
import uk.gov.hmrc.crdlcache.models.{CodeListCode, CodeListEntry, CorrespondenceListInstruction}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.play.json.Codecs

import java.time.Instant
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class CorrespondenceListsRepository @Inject() (mongoComponent: MongoComponent)(using
  ec: ExecutionContext
) extends CodeListsRepository[(String, String), CorrespondenceListInstruction](
    mongoComponent,
    collectionName = "correspondenceLists",
    extraCodecs =
      Codecs.playFormatSumCodecs[JsValue](Format(Reads.JsValueReads, Writes.jsValueWrites)) ++
        Codecs.playFormatSumCodecs[JsBoolean](Format(Reads.JsBooleanReads, Writes.jsValueWrites)),
    indexes = Seq(
      IndexModel(
        Indexes.ascending("codeListCode", "key", "value", "activeFrom"),
        IndexOptions().unique(true)
      )
    )
  )
  with Logging {

  override def activationDate(instruction: CorrespondenceListInstruction): Instant =
    instruction.activeFrom

  override def keyOfEntry(codeListEntry: CodeListEntry): (String, String) =
    (codeListEntry.key, codeListEntry.value)

  override def filtersForKey(key: (String, String)): Seq[Bson] =
    Seq(equal("key", key._1), equal("value", key._2))

  private def deleteCorrespondenceListEntries(
    session: ClientSession,
    codeListCode: CodeListCode
  ): Future[Unit] =
    collection
      .deleteMany(
        session,
        equal("codeListCode", codeListCode.code)
      )
      .toFuture()
      .map { result =>
        if (!result.wasAcknowledged())
          throw MongoError.NotAcknowledged
      }

  def saveEntries(
    session: ClientSession,
    codeListCode: CodeListCode,
    entries: List[CodeListEntry]
  ): Future[Unit] =
    for {
      _ <- deleteCorrespondenceListEntries(session, codeListCode)

      _ <- collection.insertMany(session, entries).toFuture().map { result =>
        if (!result.wasAcknowledged())
          throw MongoError.NotAcknowledged
      }
    } yield ()

  def executeInstruction(
    session: ClientSession,
    instruction: CorrespondenceListInstruction
  ): Future[Unit] =
    instruction match {
      case CorrespondenceListInstruction.UpsertEntry(codeListEntry) =>
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
      case CorrespondenceListInstruction.InvalidateEntry(codeListEntry) =>
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
      case CorrespondenceListInstruction.RecordMissingEntry(
            codeListCode,
            key,
            value,
            removedAt
          ) =>
        logger.debug(
          s"Recording removal of entry in correspondence list ${codeListCode.code} with key $key"
        )
        supersedePreviousEntries(
          session,
          codeListCode,
          (key, value),
          removedAt,
          includeActiveFrom = true
        )
    }
}
