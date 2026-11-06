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
import org.bson.conversions.Bson
import org.mongodb.scala.ClientSession
import org.mongodb.scala.bson.BsonNull
import org.mongodb.scala.model.*
import org.mongodb.scala.model.Filters.*
import org.mongodb.scala.model.Projections.*
import play.api.Logging
import play.api.libs.json.Format
import uk.gov.hmrc.crdlcache.models.*
import uk.gov.hmrc.crdlcache.models.CustomsOfficeListsInstruction.{
  RecordMissingCustomsOffice,
  UpsertCustomsOffice
}
import uk.gov.hmrc.crdlcache.models.errors.MongoError
import uk.gov.hmrc.crdlcache.models.formats.MongoFormats
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.play.json.{Codecs, PlayMongoRepository}
import uk.gov.hmrc.mongo.transaction.Transactions

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.concurrent.TimeUnit
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class CustomsOfficeListsRepository @Inject() (val mongoComponent: MongoComponent)(using
  ec: ExecutionContext
) extends PlayMongoRepository[CustomsOffice](
    mongoComponent,
    collectionName = "customsOfficeLists",
    domainFormat = MongoFormats.customsOfficeFormat,
    indexes = Seq(
      IndexModel(
        Indexes.ascending("domain", "phase", "referenceNumber", "activeFrom"),
        IndexOptions().unique(true)
      ),
      IndexModel(
        Indexes.ascending("domain", "phase", "countryCode", "referenceNumber", "activeFrom")
      ),
      IndexModel(
        Indexes.ascending(
          "domain",
          "phase",
          "customsOfficeTimetable.customsOfficeTimetableLine.customsOfficeRoleTrafficCompetence.roleName",
          "referenceNumber",
          "activeFrom"
        )
      ),
      IndexModel(Indexes.ascending("activeTo"), IndexOptions().expireAfter(30, TimeUnit.DAYS))
    ),
    extraCodecs = Seq(Codecs.playFormatCodec(CustomsOfficeSummary.format)),
    replaceIndexes = true
  )
  with Transactions
  with Logging {

  protected def nullOrMissing(fieldName: String) =
    or(equal(fieldName, null), exists(fieldName, false))

  def fetchCustomsOfficeReferenceNumbers(
    session: ClientSession,
    phase: Option[Phase],
    domain: Option[Domain]
  ): Future[Set[String]] =
    collection
      .find(
        session,
        and(
          domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
          phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
          equal("activeTo", null)
        )
      )
      .map(_.referenceNumber)
      .toFuture()
      .map(_.toSet)

  def upsertOffice(session: ClientSession, customsOffice: CustomsOffice) =
    collection
      .replaceOne(
        session,
        Filters.and(
          customsOffice.domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
          customsOffice.phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
          equal("referenceNumber", customsOffice.referenceNumber),
          equal("activeFrom", customsOffice.activeFrom)
        ),
        customsOffice,
        new ReplaceOptions().upsert(true)
      )
      .toFuture()
      .map { result =>
        if (!result.wasAcknowledged())
          throw MongoError.NotAcknowledged
        else if (result.getModifiedCount == 0 && result.getUpsertedId == BsonNull)
          throw MongoError.NoMatchingDocument
      }

  def supersedeOffice(
    session: ClientSession,
    referenceNumber: String,
    activeFrom: Instant,
    phase: Option[Phase],
    domain: Option[Domain],
    includeActiveFrom: Boolean
  ): Future[Unit] = {
    collection
      .updateMany(
        session,
        and(
          domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
          phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
          equal("referenceNumber", referenceNumber),
          if includeActiveFrom then lte("activeFrom", activeFrom) else lt("activeFrom", activeFrom),
          or(equal("activeTo", null), exists("activeTo", false))
        ),
        Updates.set("activeTo", activeFrom)
      )
      .toFuture()
      .map { result =>
        if (!result.wasAcknowledged())
          throw MongoError.NotAcknowledged
      }
  }

  def executeInstructions(
    session: ClientSession,
    instructions: List[CustomsOfficeListsInstruction]
  ): Future[Unit] =
    instructions.sortBy(_.activeFrom).foldLeft(Future.unit) {
      (previousInstruction, nextInstruction) =>
        previousInstruction.flatMap { _ =>
          nextInstruction match {
            case UpsertCustomsOffice(customsOffice) =>
              logger.debug(
                s"Upserting customs office with reference number: ${customsOffice.referenceNumber}"
              )
              for {
                _ <- supersedeOffice(
                  session,
                  customsOffice.referenceNumber,
                  customsOffice.activeFrom,
                  customsOffice.phase,
                  customsOffice.domain,
                  includeActiveFrom = false
                )
                _ <- upsertOffice(session, customsOffice)
              } yield ()
            case RecordMissingCustomsOffice(referenceNumber, phase, domain, removedAt) =>
              logger.debug(
                s"Recording removal of customs office reference number: $referenceNumber"
              )
              supersedeOffice(session, referenceNumber, removedAt, phase, domain, true)
          }
        }
    }

  def fetchCustomsOfficeLists(
    referenceNumbers: Option[Set[String]],
    countryCodes: Option[Set[String]],
    roles: Option[Set[String]],
    activeAt: Instant,
    phase: Option[Phase],
    domain: Option[Domain],
    roleDate: Option[LocalDate] = None
  ): Future[Seq[CustomsOffice]] = {
    val countryCodeFilters = countryCodes
      .map(countryCodes =>
        if countryCodes.nonEmpty then List(in("countryCode", countryCodes.toSeq*)) else List.empty
      )
      .getOrElse(List.empty)

    val referenceNumberFilters = referenceNumbers
      .map(referenceNumbers =>
        if referenceNumbers.nonEmpty then List(in("referenceNumber", referenceNumbers.toSeq*))
        else List.empty
      )
      .getOrElse(List.empty)

    val dateFilters =
      List(lte("activeFrom", activeAt), or(equal("activeTo", null), gt("activeTo", activeAt)))

    val roleFilters = (roles, roleDate) match {
      case (Some(r), Some(date)) if r.nonEmpty =>
        val roleDateInstant = date.atStartOfDay(ZoneOffset.UTC).toInstant
        List(
          Filters.elemMatch(
            "customsOfficeTimetable",
            and(
              lte("seasonStartDate", roleDateInstant),
              gte("seasonEndDate", roleDateInstant),
              in(
                "customsOfficeTimetableLine.customsOfficeRoleTrafficCompetence.roleName",
                r.toSeq*
              )
            )
          )
        )
      case (Some(r), None) if r.nonEmpty =>
        List(
          in(
            "customsOfficeTimetable.customsOfficeTimetableLine.customsOfficeRoleTrafficCompetence.roleName",
            r.toSeq*
          )
        )
      case _ => List.empty
    }

    val phaseDomainFilter = (phase, domain) match {
      case (Some(p), Some(d)) => List(equal("phase", p), equal("domain", d))
      case (None, None)       => List(nullOrMissing("phase"), nullOrMissing("domain"))
      case _                  => List.empty
    }

    val allFilters =
      phaseDomainFilter ++ countryCodeFilters ++ roleFilters ++ referenceNumberFilters ++ dateFilters

    collection
      .find(and(allFilters*))
      .toFuture()
  }

  def fetchCustomsOfficeByRef(
    domain: Option[Domain],
    phase: Option[Phase],
    referenceNumber: String,
    activeAt: Instant
  ): Future[Option[CustomsOffice]] =
    collection
      .find(
        and(
          domain.map(equal("domain", _)).getOrElse(nullOrMissing("domain")),
          phase.map(equal("phase", _)).getOrElse(nullOrMissing("phase")),
          equal("referenceNumber", referenceNumber),
          lte("activeFrom", activeAt),
          or(equal("activeTo", null), gt("activeTo", activeAt))
        )
      )
      .headOption()

  def summaryFilters(
    activeAt: Instant,
    referenceNumber: Option[String] = None,
    countryCode: Option[String] = None,
    officeName: Option[String] = None,
    phase: Option[Phase],
    domain: Option[Domain]
  ): List[Bson] = {

    val dateFilters = List(
      lte("activeFrom", activeAt),
      or(equal("activeTo", null), gt("activeTo", activeAt))
    )

    val referenceNumberFilter = referenceNumber.map(r => regex("referenceNumber", r, "i")).toList
    val countryCodeFilter     = countryCode.map(c => equal("countryCode", c)).toList
    val officeNameFilter =
      officeName.map(n => regex("customsOfficeLsd.customsOfficeUsualName", n, "i")).toList

    val phaseDomainFilters = (phase, domain) match {
      case (Some(p), Some(d)) => List(equal("phase", p), equal("domain", d))
      case _                  => List.empty
    }

    phaseDomainFilters ++ countryCodeFilter ++ referenceNumberFilter ++ dateFilters ++ officeNameFilter
  }

  def fetchCustomsOfficeSummaries(
    activeAt: Instant,
    pageNum: Int,
    pageSize: Int,
    referenceNumber: Option[String] = None,
    countryCode: Option[String] = None,
    officeName: Option[String] = None,
    phase: Option[Phase],
    domain: Option[Domain]
  ): Future[Seq[CustomsOfficeSummary]] =
    collection
      .find[CustomsOfficeSummary](
        and(
          summaryFilters(
            activeAt,
            referenceNumber,
            countryCode,
            officeName,
            phase,
            domain
          )*
        )
      )
      .projection(
        fields(
          include(
            "referenceNumber",
            "countryCode",
            "customsOfficeLsd.customsOfficeUsualName",
            "phase",
            "domain"
          ),
          excludeId()
        )
      )
      .skip((pageNum - 1) * pageSize)
      .limit(pageSize)
      .toFuture()

  def customsOfficesCount(
    activeAt: Instant,
    referenceNumber: Option[String] = None,
    countryCode: Option[String] = None,
    officeName: Option[String] = None,
    phase: Option[Phase],
    domain: Option[Domain]
  ): Future[Long] =
    collection
      .countDocuments(
        and(summaryFilters(activeAt, referenceNumber, countryCode, officeName, phase, domain)*)
      )
      .toFuture()
}
