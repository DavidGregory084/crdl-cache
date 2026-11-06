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

import com.mongodb.client.model.ReplaceOptions
import org.bson.codecs.Codec
import org.mongodb.scala.*
import org.mongodb.scala.bson.BsonNull
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.*
import org.mongodb.scala.model.Filters.*
import play.api.libs.json.*
import uk.gov.hmrc.crdlcache.models
import uk.gov.hmrc.crdlcache.models.CodeListCode.code
import uk.gov.hmrc.crdlcache.models.errors.MongoError
import uk.gov.hmrc.crdlcache.models.formats.MongoFormats
import uk.gov.hmrc.crdlcache.models.{CodeListCode, CodeListEntry, Domain, Phase}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.play.json.PlayMongoRepository
import uk.gov.hmrc.mongo.transaction.Transactions

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}

abstract class CodeListsRepository[K, I](
  val mongoComponent: MongoComponent,
  collectionName: String,
  indexes: Seq[IndexModel],
  extraCodecs: Seq[Codec[?]]
)(using
  ec: ExecutionContext
) extends PlayMongoRepository[CodeListEntry](
    mongoComponent,
    collectionName,
    domainFormat = MongoFormats.codeListEntryFormat,
    extraCodecs = extraCodecs,
    indexes = IndexModel(
      Indexes.ascending("activeTo"),
      IndexOptions().expireAfter(30, TimeUnit.DAYS)
    ) +: indexes,
    replaceIndexes = true
  )
  with Transactions {

  def activationDate(instruction: I): Instant

  def keyOfEntry(codeListEntry: CodeListEntry): K

  def filtersForKey(codeListCode: CodeListCode, key: K): Seq[Bson]

  def executeInstruction(session: ClientSession, instruction: I): Future[Unit]

  def executeInstructions(session: ClientSession, instructions: List[I]): Future[Unit] =
    instructions.sortBy(activationDate).foldLeft(Future.unit) {
      (previousInstruction, nextInstruction) =>
        previousInstruction.flatMap(_ => executeInstruction(session, nextInstruction))
    }

  protected def nullOrMissing(fieldName: String) =
    or(equal(fieldName, null), exists(fieldName, false))

  def fetchEntryKeys(
    session: ClientSession,
    code: CodeListCode,
    phase: Option[Phase] = None,
    domain: Option[Domain] = None
  ): Future[Set[K]] =
    collection
      .find(
        session,
        and(
          domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
          phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
          equal("codeListCode", code.code),
          equal("activeTo", null)
        )
      )
      .map(keyOfEntry)
      .toFuture()
      .map(_.toSet)

  def fetchEntries(
    code: CodeListCode,
    filterKeys: Option[Set[String]],
    filterProperties: Option[Map[String, JsValue]],
    activeAt: Instant,
    phase: Option[Phase],
    domain: Option[Domain]
  ): Future[Seq[CodeListEntry]] = {
    val listFilters = List(
      domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
      phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
      equal("codeListCode", code.code)
    )

    val keyFilters = filterKeys
      .map { ks =>
        if ks.nonEmpty
        then List(in("key", ks.toSeq*))
        else List.empty
      }
      .getOrElse(List.empty)

    val dateFilters = List(
      lte("activeFrom", activeAt),
      or(equal("activeTo", null), gt("activeTo", activeAt))
    )

    val propertyFilters = filterProperties
      .map { props =>
        if props.nonEmpty
        then props.map((k, v) => equal(s"properties.$k", v))
        else List.empty
      }
      .getOrElse(List.empty)

    val allFilters = listFilters ++ keyFilters ++ dateFilters ++ propertyFilters

    collection
      .find(and(allFilters*))
      .sort(Sorts.ascending("key"))
      .toFuture()
  }

  private def entryFilters(
    code: CodeListCode,
    activeAt: Instant,
    key: Option[String],
    value: Option[String]
  ): List[Bson] = {
    val mandatoryFilters = List(
      equal("codeListCode", code.code),
      lte("activeFrom", activeAt),
      or(equal("activeTo", null), gt("activeTo", activeAt))
    )
    val keyFilter   = key.map(k => regex("key", k, "i")).toList
    val valueFilter = value.map(v => regex("value", v, "i")).toList
    mandatoryFilters ++ keyFilter ++ valueFilter
  }

  def fetchEntriesPaged(
    code: CodeListCode,
    activeAt: Instant,
    pageNum: Int,
    pageSize: Int,
    key: Option[String] = None,
    value: Option[String] = None
  ): Future[Seq[CodeListEntry]] =
    collection
      .find(and(entryFilters(code, activeAt, key, value)*))
      .sort(Sorts.ascending("key"))
      .skip((pageNum - 1) * pageSize)
      .limit(pageSize)
      .toFuture()

  def countEntries(
    code: CodeListCode,
    activeAt: Instant,
    key: Option[String] = None,
    value: Option[String] = None
  ): Future[Long] =
    collection
      .countDocuments(and(entryFilters(code, activeAt, key, value)*))
      .toFuture()

  protected def supersedePreviousEntries(
    session: ClientSession,
    codeListCode: CodeListCode,
    key: K,
    activeFrom: Instant,
    includeActiveFrom: Boolean
  ): Future[Unit] = {
    collection
      .updateMany(
        session,
        and(
          filtersForKey(codeListCode, key) ++
            Seq(
              if includeActiveFrom
              then lte("activeFrom", activeFrom)
              else lt("activeFrom", activeFrom),
              nullOrMissing("activeTo")
            )*
        ),
        Updates.set("activeTo", activeFrom)
      )
      .toFuture()
      .map { result =>
        if (!result.wasAcknowledged())
          throw MongoError.NotAcknowledged
      }
  }

  protected def upsertEntry(session: ClientSession, codeListEntry: CodeListEntry): Future[Unit] =
    collection
      .replaceOne(
        session,
        and(
          filtersForKey(codeListEntry.codeListCode, keyOfEntry(codeListEntry)) ++
            Seq(equal("activeFrom", codeListEntry.activeFrom))*
        ),
        codeListEntry,
        new ReplaceOptions().upsert(true)
      )
      .toFuture()
      .map { result =>
        if (!result.wasAcknowledged())
          throw MongoError.NotAcknowledged
        else if (result.getModifiedCount == 0 && result.getUpsertedId == BsonNull)
          throw MongoError.NoMatchingDocument
      }
}
