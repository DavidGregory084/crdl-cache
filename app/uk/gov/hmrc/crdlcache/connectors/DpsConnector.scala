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

package uk.gov.hmrc.crdlcache.connectors

import com.typesafe.config.Config
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl
import org.apache.pekko.stream.scaladsl.Source
import play.api.Logging
import uk.gov.hmrc.crdlcache.config.AppConfig
import uk.gov.hmrc.crdlcache.models.CodeListCode.code
import uk.gov.hmrc.crdlcache.models.dps.codelist.CodeListResponse
import uk.gov.hmrc.crdlcache.models.dps.col.CustomsOfficeListResponse
import uk.gov.hmrc.crdlcache.models.{CodeListCode, Domain, Phase}
import uk.gov.hmrc.http.HttpReads.Implicits.*
import uk.gov.hmrc.http.UpstreamErrorResponse.{Upstream4xxResponse, Upstream5xxResponse}
import uk.gov.hmrc.http.client.HttpClientV2
import uk.gov.hmrc.http.{
  HeaderCarrier,
  HeaderNames,
  HttpReads,
  Retries,
  StringContextOps,
  UpstreamErrorResponse
}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.{Base64, UUID}
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class DpsConnector @Inject() (httpClient: HttpClientV2, appConfig: AppConfig)(using
  system: ActorSystem
) extends Retries
  with Logging {
  override protected def actorSystem: ActorSystem = system
  override protected def configuration: Config    = appConfig.config.underlying

  private val base64Encoder = Base64.getEncoder
  private val dateFormatter = DateTimeFormatter.ISO_INSTANT

  private val baseRefDataUrl = url"${appConfig.dpsUrl}/${appConfig.dpsRefDataPath.split('/')}"
  private val baseCustomsOfficeUrl =
    url"${appConfig.dpsUrl}/${appConfig.dpsCustomsOfficesPath.split('/')}"

  private lazy val basicAuthSecret = {
    val clientIdAndSecret =
      s"${appConfig.dpsClientId}:${appConfig.dpsClientSecret}"
    val authSecret =
      base64Encoder.encodeToString(clientIdAndSecret.getBytes(StandardCharsets.UTF_8))
    s"Basic $authSecret"
  }

  private def fetchCodeListSnapshot(
    code: CodeListCode,
    lastUpdatedDate: Instant,
    startIndex: Int,
    businessDomainPhase: Option[Phase] = None,
    businessDomain: Option[Domain] = None
  )(using ec: ExecutionContext): Future[CodeListResponse] = {
    val baseQueryParams = Map(
      "code_list_code"    -> code.code,
      "last_updated_date" -> dateFormatter.format(lastUpdatedDate),
      "$start_index"      -> startIndex,
      "$count"            -> 10,
      "$orderby"          -> "snapshotversion ASC"
    )

    val queryParams = baseQueryParams ++
      businessDomainPhase.map("business_domain_phase" -> _).toMap ++
      businessDomain.map("business_domain" -> _).toMap

    val dpsUrl        = url"$baseRefDataUrl?$queryParams"
    val correlationId = UUID.randomUUID().toString

    logger.info(
      s"Fetching ${code.code} snapshots at index $startIndex as of $lastUpdatedDate with correlationId $correlationId"
    )

    val fetchResult =
      retryFor(s"fetch of ${code.code} snapshots at index $startIndex as of $lastUpdatedDate") {
        // No point in retrying if our request is wrong
        case Upstream4xxResponse(_) => false
        // Attempt to recover from intermittent connectivity issues
        case Upstream5xxResponse(_) => true
      } {
        httpClient
          .get(dpsUrl)(HeaderCarrier())
          .setHeader(
            "correlationId"           -> correlationId,
            HeaderNames.authorisation -> basicAuthSecret
          )
          .execute[CodeListResponse](using throwOnFailure(readEitherOf[CodeListResponse]), ec)
      }
    fetchResult.failed.foreach(err =>
      logger.error(
        s"Retries exceeded while fetching ${code.code} snapshots at index $startIndex as of $lastUpdatedDate with correlationId $correlationId ",
        err
      )
    )
    fetchResult
  }

  def fetchCodeListSnapshots(
    code: CodeListCode,
    lastUpdatedDate: Instant,
    businessDomainPhase: Option[Phase] = None,
    businessDomain: Option[Domain] = None
  )(using
    ec: ExecutionContext
  ): Source[CodeListResponse, NotUsed] = Source
    .unfoldAsync[Int, CodeListResponse](0) { startIndex =>
      fetchCodeListSnapshot(code, lastUpdatedDate, startIndex, businessDomainPhase, businessDomain)
        .map { response =>
          if (response.elements.isEmpty)
            None
          else
            Some((startIndex + 10, response))
        }
    }

  def fetchCustomsOfficeLists(
    phase: Option[Phase] = None,
    domain: Option[Domain] = None
  )(using
    ec: ExecutionContext
  ): Source[CustomsOfficeListResponse, NotUsed] = Source
    .unfoldAsync[Int, CustomsOfficeListResponse](0) { startIndex =>
      fetchCustomsOfficeList(startIndex, phase, domain).map { response =>
        if (response.elements.isEmpty)
          None
        else
          Some((startIndex + appConfig.customsOfficesPageSize, response))
      }
    }

  private def fetchCustomsOfficeList(
    startIndex: Int,
    phase: Option[Phase] = None,
    domain: Option[Domain] = None
  )(using ec: ExecutionContext): Future[CustomsOfficeListResponse] = {
    val baseQueryParams = Map(
      "$start_index" -> startIndex,
      "$count"       -> appConfig.customsOfficesPageSize,
      "iscurrentindicator" -> 1 // Possible improvement suggested by DPS that should have their API prefilter to the latest version
    )

    val queryParams = baseQueryParams ++
      phase.map("business_domain_phase" -> _).toMap ++
      domain.map("business_domain" -> _).toMap

    val correlationId = UUID.randomUUID().toString
    val dpsUrl        = url"$baseCustomsOfficeUrl?$queryParams"

    logger.info(
      s"Fetching customs office list at index $startIndex with correlationId $correlationId"
    )

    val fetchResult = retryFor(s"fetch of customs office list at index $startIndex") {
      // No point in retrying if our request is wrong
      case Upstream4xxResponse(_) => false
      // Attempt to recover from intermittent connectivity issues
      case Upstream5xxResponse(_) => true
    } {
      httpClient
        .get(dpsUrl)(HeaderCarrier())
        .setHeader(
          "correlationId"           -> correlationId,
          HeaderNames.authorisation -> basicAuthSecret
        )
        .execute[CustomsOfficeListResponse](using
          throwOnFailure(readEitherOf[CustomsOfficeListResponse]),
          ec
        )
    }
    fetchResult.failed.foreach(err =>
      logger.error(
        s"Retries exceeded while fetching customs office list at index $startIndex with correlationId $correlationId ",
        err
      )
    )
    fetchResult
  }
}
