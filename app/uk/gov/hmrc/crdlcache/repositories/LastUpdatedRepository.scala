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

import com.mongodb.client.model.{IndexModel, IndexOptions, UpdateOptions}
import org.mongodb.scala.*
import org.mongodb.scala.bson.BsonNull
import org.mongodb.scala.model.Filters.*
import org.mongodb.scala.model.{Filters, Indexes, Sorts, Updates}
import uk.gov.hmrc.crdlcache.models.CodeListCode.code
import uk.gov.hmrc.crdlcache.models.errors.MongoError
import uk.gov.hmrc.crdlcache.models.formats.MongoFormats
import uk.gov.hmrc.crdlcache.models.{CodeListCode, Domain, LastUpdated, Phase}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.play.json.PlayMongoRepository
import uk.gov.hmrc.mongo.transaction.Transactions

import java.time.Instant
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class LastUpdatedRepository @Inject() (val mongoComponent: MongoComponent)(using
  ec: ExecutionContext
) extends PlayMongoRepository[LastUpdated](
    mongoComponent,
    collectionName = "last-updated",
    domainFormat = MongoFormats.lastUpdatedFormat,
    indexes = Seq(
      IndexModel(Indexes.ascending("domain", "phase", "codeListCode"), IndexOptions().unique(true)),
      IndexModel(Indexes.ascending("codeListCode"), IndexOptions().unique(false)),
      IndexModel(Indexes.ascending("phase"), IndexOptions().unique(false)),
      IndexModel(Indexes.ascending("domain"), IndexOptions().unique(false))
    ),
    replaceIndexes = true
  )
  with Transactions {

  // This collection contains only one document per codelist
  override lazy val requiresTtlIndex: Boolean = false

  protected def nullOrMissing(fieldName: String) =
    or(equal(fieldName, null), exists(fieldName, false))

  def fetchLastUpdated(
    codeListCode: CodeListCode,
    phase: Option[Phase],
    domain: Option[Domain]
  ): Future[Option[LastUpdated]] = {
    collection
      .find(
        and(
          domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
          phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
          equal("codeListCode", codeListCode.code)
        )
      )
      .headOption()
  }

  def fetchAllLastUpdated: Future[Seq[LastUpdated]] = {
    collection.find().toFuture()
  }

  def setLastUpdated(
    session: ClientSession,
    codeListCode: CodeListCode,
    snapshotVersion: Long,
    phase: Option[Phase],
    domain: Option[Domain],
    lastUpdated: Instant
  ): Future[Unit] = {
    val filter = (phase, domain) match {
      case (Some(phase), Some(domain)) =>
        and(
          equal("domain", domain),
          equal("phase", phase),
          equal("codeListCode", codeListCode.code)
        )
      case (None, None) =>
        and(
          nullOrMissing("domain"),
          nullOrMissing("phase"),
          equal("codeListCode", codeListCode.code)
        )
      case _ =>
        throw Exception(
          "Both phase and domain must be provided together, or neither should be provided"
        )
    }

    collection
      .updateOne(
        session,
        filter,
        Updates.combine(
          Updates.set("lastUpdated", lastUpdated),
          Updates.set("snapshotVersion", snapshotVersion)
        ),
        UpdateOptions().upsert(true)
      )
      .toFuture()
      .map { result =>
        if (!result.wasAcknowledged())
          throw MongoError.NotAcknowledged
        else if (result.getModifiedCount == 0 && result.getUpsertedId == BsonNull)
          throw MongoError.NoMatchingDocument
      }
  }

  private def snapshotFilters(
    codeListCode: Option[CodeListCode],
    phase: Option[Phase],
    domain: Option[Domain]
  ): Seq[org.mongodb.scala.bson.conversions.Bson] = {
    val domainFilter = domain.map(equal("domain", _)).toSeq
    val phaseFilter  = phase.map(equal("phase", _)).toSeq
    val codeFilter   = codeListCode.map(c => regex("codeListCode", c.code, "i")).toSeq
    domainFilter ++ phaseFilter ++ codeFilter
  }

  def fetchAllLastUpdatedPaged(
    pageNum: Int,
    pageSize: Int,
    codeListCode: Option[CodeListCode] = None,
    phase: Option[Phase] = None,
    domain: Option[Domain] = None
  ): Future[Seq[LastUpdated]] = {
    val filters = snapshotFilters(codeListCode, phase, domain)
    val query   = if (filters.isEmpty) collection.find() else collection.find(and(filters*))
    query
      .sort(Sorts.ascending("codeListCode"))
      .skip((pageNum - 1) * pageSize)
      .limit(pageSize)
      .toFuture()
  }

  def codeListCount(
    codeListCode: Option[CodeListCode] = None,
    phase: Option[Phase] = None,
    domain: Option[Domain] = None
  ): Future[Long] = {
    val filters = snapshotFilters(codeListCode, phase, domain)
    if (filters.isEmpty) collection.countDocuments().toFuture()
    else collection.countDocuments(and(filters*)).toFuture()
  }
}
