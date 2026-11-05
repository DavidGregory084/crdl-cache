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

package uk.gov.hmrc.crdlcache.controllers

import play.api.libs.json.Json
import play.api.mvc.{Action, AnyContent, ControllerComponents}
import uk.gov.hmrc.crdlcache.controllers.auth.Permissions.ReadCustomsOfficeLists
import uk.gov.hmrc.crdlcache.models.{Domain, Phase}
import uk.gov.hmrc.crdlcache.models.formats.HttpFormats
import uk.gov.hmrc.crdlcache.models.paging.PagedResult
import uk.gov.hmrc.crdlcache.repositories.CustomsOfficeListsRepository
import uk.gov.hmrc.internalauth.client.*
import uk.gov.hmrc.play.bootstrap.backend.controller.BackendController

import java.time.{Clock, Instant, LocalDate}
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton()
class CustomsOfficeListsController @Inject() (
  cc: ControllerComponents,
  auth: BackendAuthComponents,
  customsOfficeListsRepository: CustomsOfficeListsRepository,
  clock: Clock
)(using ec: ExecutionContext)
  extends BackendController(cc)
  with HttpFormats {

  private def validatePhaseDomain(phase: Option[Phase], domain: Option[Domain]): Option[String] =
    (phase, domain) match {
      case (Some(_), None) | (None, Some(_)) =>
        Some("Both phase and domain must be provided together, or neither should be provided")
      case _ => None
    }

  def fetchCustomsOfficeLists(
    referenceNumbers: Option[Set[String]],
    countryCodes: Option[Set[String]],
    roles: Option[Set[String]],
    activeAt: Option[Instant],
    phase: Option[Phase],
    domain: Option[Domain],
    roleDate: Option[LocalDate]
  ): Action[AnyContent] = auth.authorizedAction(ReadCustomsOfficeLists).async { _ =>
    if (roleDate.isDefined && roles.isEmpty)
      Future.successful(
        BadRequest(
          Json.obj("statusCode" -> 400, "message" -> "roleDate requires roles to be specified")
        )
      )
    else
      validatePhaseDomain(phase, domain) match {
        case Some(errorMessage) =>
          Future.successful(BadRequest(Json.obj("error" -> errorMessage)))
        case None =>
          customsOfficeListsRepository
            .fetchCustomsOfficeLists(
              referenceNumbers,
              countryCodes,
              roles,
              activeAt.getOrElse(clock.instant()),
              phase,
              domain,
              roleDate
            )
            .map { customsOfficeLists =>
              Ok(Json.toJson(customsOfficeLists))
            }
      }
  }

  def fetchCustomsOfficeDetail(
    referenceNumber: String,
    activeAt: Option[Instant]
  ): Action[AnyContent] = auth.authorizedAction(ReadCustomsOfficeLists).async { _ =>
    val resolvedActiveAt = activeAt.getOrElse(clock.instant())
    customsOfficeListsRepository.fetchCustomsOfficeByRef(referenceNumber, resolvedActiveAt).map {
      case Some(office) => Ok(Json.toJson(office))
      case None         => NotFound
    }
  }

  def fetchCustomsOfficeListSummaries(
    pageNum: Int,
    pageSize: Int,
    activeAt: Option[Instant],
    referenceNumber: Option[String],
    countryCode: Option[String],
    officeName: Option[String],
    phase: Option[Phase],
    domain: Option[Domain]
  ): Action[AnyContent] = auth.authorizedAction(ReadCustomsOfficeLists).async { _ =>
    val resolvedActiveAt = activeAt.getOrElse(clock.instant())
    val summariesFuture = customsOfficeListsRepository
      .fetchCustomsOfficeSummaries(
        activeAt.getOrElse(clock.instant()),
        pageNum,
        pageSize,
        referenceNumber,
        countryCode,
        officeName,
        phase,
        domain
      )
    val officesCountFuture = customsOfficeListsRepository.customsOfficesCount(
      resolvedActiveAt,
      referenceNumber,
      countryCode,
      officeName,
      phase,
      domain
    )

    for {
      summaries    <- summariesFuture
      officesCount <- officesCountFuture
    } yield {
      Ok(
        Json.toJson(
          PagedResult(
            pageNum = pageNum,
            pageSize = pageSize,
            itemsInPage = summaries.length,
            totalItems = officesCount,
            totalPages = Math.ceil(officesCount.toFloat / pageSize).toInt,
            items = summaries
          )
        )
      )
    }
  }
}
