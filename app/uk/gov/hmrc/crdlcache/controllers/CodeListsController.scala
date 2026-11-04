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

import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, AnyContent, ControllerComponents}
import uk.gov.hmrc.crdlcache.controllers.auth.Permissions.ReadCodeLists
import uk.gov.hmrc.crdlcache.models.CodeListCode.isCorrespondenceList
import uk.gov.hmrc.crdlcache.models.formats.HttpFormats
import uk.gov.hmrc.crdlcache.models.paging.PagedResult
import uk.gov.hmrc.crdlcache.models.{CodeListCode, Domain, Phase}
import uk.gov.hmrc.crdlcache.repositories.{
  CorrespondenceListsRepository,
  LastUpdatedRepository,
  StandardCodeListsRepository
}
import uk.gov.hmrc.internalauth.client.*
import uk.gov.hmrc.play.bootstrap.backend.controller.BackendController

import java.time.{Clock, Instant}
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton()
class CodeListsController @Inject() (
  cc: ControllerComponents,
  auth: BackendAuthComponents,
  codeListsRepository: StandardCodeListsRepository,
  correspondenceListsRepository: CorrespondenceListsRepository,
  lastUpdatedRepository: LastUpdatedRepository,
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

  def fetchCodeListEntries(
    code: String,
    filterKeys: Option[Set[String]],
    filterProperties: Option[Map[String, JsValue]],
    activeAt: Option[Instant],
    phase: Option[Phase],
    domain: Option[Domain]
  ): Action[AnyContent] =
    auth.authorizedAction(ReadCodeLists).async { _ =>
      validatePhaseDomain(phase, domain) match {
        case Some(errorMessage) =>
          Future.successful(
            BadRequest(Json.obj("error" -> errorMessage))
          )
        case None =>
          val codeListCode = CodeListCode(code)
          val codeListEntries =
            if (codeListCode.isCorrespondenceList)
              correspondenceListsRepository
                .fetchEntries(
                  codeListCode,
                  filterKeys,
                  filterProperties,
                  activeAt.getOrElse(clock.instant()),
                  phase,
                  domain
                )
            else
              codeListsRepository
                .fetchEntries(
                  codeListCode,
                  filterKeys,
                  filterProperties,
                  activeAt.getOrElse(clock.instant()),
                  phase,
                  domain
                )

          codeListEntries.map { entries =>
            Ok(Json.toJson(entries))
          }
      }
    }

  def fetchCodeListEntriesPaginated(
    code: String,
    pageNum: Int,
    pageSize: Int,
    activeAt: Option[Instant],
    key: Option[String],
    value: Option[String]
  ): Action[AnyContent] = auth.authorizedAction(ReadCodeLists).async { _ =>
    val codeListCode     = CodeListCode(code)
    val resolvedActiveAt = activeAt.getOrElse(clock.instant())
    val repo =
      if (codeListCode.isCorrespondenceList) correspondenceListsRepository else codeListsRepository
    val entriesFuture =
      repo.fetchEntriesPaged(codeListCode, resolvedActiveAt, pageNum, pageSize, key, value)
    val countFuture = repo.countEntries(codeListCode, resolvedActiveAt, key, value)
    for {
      entries <- entriesFuture
      count   <- countFuture
    } yield {
      Ok(
        Json.toJson(
          PagedResult(
            items = entries,
            pageNum = pageNum,
            pageSize = pageSize,
            itemsInPage = entries.length,
            totalItems = count,
            totalPages = Math.ceil(count.toFloat / pageSize).toInt
          )
        )
      )
    }
  }

  def fetchCodeListVersions: Action[AnyContent] =
    auth.authorizedAction(ReadCodeLists).async { _ =>
      lastUpdatedRepository.fetchAllLastUpdated
        .map { lastUpdatedEntries =>
          Ok(Json.toJson(lastUpdatedEntries))
        }
    }

  def fetchCodeListVersionsPaged(
    pageNum: Int,
    pageSize: Int,
    code: Option[String],
    phase: Option[Phase],
    domain: Option[Domain]
  ): Action[AnyContent] =
    auth.authorizedAction(ReadCodeLists).async { _ =>
      val codeListCode = code.map(CodeListCode.apply)
      val listUpdatedFuture =
        lastUpdatedRepository.fetchAllLastUpdatedPaged(
          pageNum,
          pageSize,
          codeListCode,
          phase,
          domain
        )
      val codeListCountFuture = lastUpdatedRepository.codeListCount(codeListCode, phase, domain)
      for {
        listUpdated   <- listUpdatedFuture
        codeListCount <- codeListCountFuture
      } yield {
        Ok(
          Json.toJson(
            PagedResult(
              items = listUpdated,
              pageNum = pageNum,
              pageSize = pageSize,
              itemsInPage = listUpdated.length,
              totalItems = codeListCount,
              totalPages = Math.ceil(codeListCount.toFloat / pageSize).toInt
            )
          )
        )
      }
    }

  def getSnapshot(
    code: String,
    phase: Option[Phase] = None,
    domain: Option[Domain] = None
  ): Action[AnyContent] = auth.authorizedAction(ReadCodeLists).async { _ =>
    lastUpdatedRepository.fetchLastUpdated(CodeListCode(code), phase, domain).map {
      case Some(snapshot) => Ok(Json.toJson(snapshot))
      case None           => NotFound
    }
  }
}
