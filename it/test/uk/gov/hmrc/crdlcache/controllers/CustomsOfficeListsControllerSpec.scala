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

import org.mockito.ArgumentMatchers.eq as equalTo
import org.mockito.Mockito.{reset, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.play.guice.GuiceOneServerPerSuite
import play.api.Application
import play.api.http.{HeaderNames, Status}
import play.api.inject.bind
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.json.Json
import play.api.libs.json.Json.*
import uk.gov.hmrc.crdlcache.controllers.auth.Permissions.ReadCustomsOfficeLists
import uk.gov.hmrc.crdlcache.models.*
import uk.gov.hmrc.crdlcache.repositories.CustomsOfficeListsRepository
import uk.gov.hmrc.http.HttpReads.Implicits.*
import uk.gov.hmrc.http.client.HttpClientV2
import uk.gov.hmrc.http.test.HttpClientV2Support
import uk.gov.hmrc.http.{HeaderCarrier, HttpResponse, StringContextOps, UpstreamErrorResponse}
import uk.gov.hmrc.internalauth.client.modules.InternalAuthModule
import uk.gov.hmrc.internalauth.client.test.StubBehaviour
import uk.gov.hmrc.internalauth.client.{BackendAuthComponents, Retrieval}

import java.time.*
import java.time.format.DateTimeFormatter
import scala.concurrent.{ExecutionContext, Future}

class CustomsOfficeListsControllerSpec
  extends AnyFlatSpec
  with Matchers
  with MockitoSugar
  with ScalaFutures
  with IntegrationPatience
  with HttpClientV2Support
  with GuiceOneServerPerSuite
  with BeforeAndAfterEach {

  given ExecutionContext = ExecutionContext.global
  given HeaderCarrier    = HeaderCarrier()

  private val authStub   = mock[StubBehaviour]
  private val repository = mock[CustomsOfficeListsRepository]

  private val fixedInstant = Instant.parse("2025-06-05T00:00:00Z")

  override def beforeEach(): Unit = {
    reset(authStub)
    reset(repository)
  }

  override def fakeApplication(): Application =
    GuiceApplicationBuilder()
      .disable[InternalAuthModule]
      .overrides(
        bind[StubBehaviour].toInstance(authStub),
        bind[BackendAuthComponents].toProvider[BackendAuthStubProvider],
        bind[CustomsOfficeListsRepository].toInstance(repository),
        bind[HttpClientV2].toInstance(httpClientV2),
        bind[Clock].toInstance(Clock.fixed(fixedInstant, ZoneOffset.UTC))
      )
      .build()
  private val dateFormat                      = DateTimeFormatter.ofPattern("yyyyMMdd")
  protected val timeFormat: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_TIME

  private val testReferenceNumber = "DK003102"

  private val testOffice = CustomsOffice(
    testReferenceNumber,
    Instant.parse("2025-03-22T00:00:00Z"),
    None,
    None,
    None,
    Some("DK003102"),
    Some("DK003102"),
    None,
    "DK",
    Some("test@dk"),
    None,
    None,
    None,
    "9850",
    Some("+45 342234 34543"),
    None,
    None,
    None,
    None,
    false,
    None,
    None,
    List("SN0009"),
    CustomsOfficeDetail(
      "Hirtshals Toldekspedition",
      "DA",
      "Hirtshals",
      false,
      None,
      None,
      false,
      "Dalsagervej 7"
    ),
    List(
      CustomsOfficeTimetable(
        1,
        None,
        LocalDate.parse("20180101", dateFormat),
        LocalDate.parse("20991231", dateFormat),
        List(
          TimetableLine(
            DayOfWeek.of(1),
            LocalTime.parse("08:00", timeFormat),
            LocalTime.parse("16:00", timeFormat),
            DayOfWeek.of(5),
            None,
            None,
            List(
              RoleTrafficCompetence("EXL", "P"),
              RoleTrafficCompetence("EXL", "R"),
              RoleTrafficCompetence("EXP", "P"),
              RoleTrafficCompetence("EXP", "R"),
              RoleTrafficCompetence("EXT", "P"),
              RoleTrafficCompetence("EXT", "R"),
              RoleTrafficCompetence("PLA", "R"),
              RoleTrafficCompetence("RFC", "R"),
              RoleTrafficCompetence("DIS", "N/A"),
              RoleTrafficCompetence("IPR", "N/A"),
              RoleTrafficCompetence("ENQ", "P"),
              RoleTrafficCompetence("ENQ", "R"),
              RoleTrafficCompetence("ENQ", "N/A"),
              RoleTrafficCompetence("REC", "P"),
              RoleTrafficCompetence("REC", "R"),
              RoleTrafficCompetence("REC", "N/A")
            )
          )
        )
      )
    ),
    None,
    None
  )

  private val testOffices = List(
    testOffice
  )

  private val testOfficePD = List(
    CustomsOffice(
      "DK003102",
      Instant.parse("2025-03-22T00:00:00Z"),
      None,
      None,
      None,
      Some("DK003102"),
      Some("DK003102"),
      None,
      "DK",
      Some("test@dk"),
      None,
      None,
      None,
      "9850",
      Some("+45 342234 34543"),
      None,
      None,
      None,
      None,
      false,
      None,
      None,
      List("SN0009"),
      CustomsOfficeDetail(
        "Hirtshals Toldekspedition",
        "DA",
        "Hirtshals",
        false,
        None,
        None,
        false,
        "Dalsagervej 7"
      ),
      List(
        CustomsOfficeTimetable(
          1,
          None,
          LocalDate.parse("20180101", dateFormat),
          LocalDate.parse("20991231", dateFormat),
          List(
            TimetableLine(
              DayOfWeek.of(1),
              LocalTime.parse("08:00", timeFormat),
              LocalTime.parse("16:00", timeFormat),
              DayOfWeek.of(5),
              None,
              None,
              List(
                RoleTrafficCompetence("EXL", "P"),
                RoleTrafficCompetence("EXL", "R"),
                RoleTrafficCompetence("EXP", "P"),
                RoleTrafficCompetence("EXP", "R"),
                RoleTrafficCompetence("EXT", "P"),
                RoleTrafficCompetence("EXT", "R"),
                RoleTrafficCompetence("PLA", "R"),
                RoleTrafficCompetence("RFC", "R"),
                RoleTrafficCompetence("DIS", "N/A"),
                RoleTrafficCompetence("IPR", "N/A"),
                RoleTrafficCompetence("ENQ", "P"),
                RoleTrafficCompetence("ENQ", "R"),
                RoleTrafficCompetence("ENQ", "N/A"),
                RoleTrafficCompetence("REC", "P"),
                RoleTrafficCompetence("REC", "R"),
                RoleTrafficCompetence("REC", "N/A")
              )
            )
          )
        )
      ),
      Some(Phase.NCTS6),
      Some(Domain.NCTS)
    )
  )

  val testOfficeJson = Json.obj(
    "referenceNumber"                             -> testReferenceNumber,
    "referenceNumberMainOffice"                   -> null,
    "referenceNumberHigherAuthority"              -> null,
    "referenceNumberCompetentAuthorityOfEnquiry"  -> "DK003102",
    "referenceNumberCompetentAuthorityOfRecovery" -> "DK003102",
    "referenceNumberTakeover"                     -> null,
    "countryCode"                                 -> "DK",
    "emailAddress"                                -> "test@dk",
    "unLocodeId"                                  -> null,
    "nctsEntryDate"                               -> null,
    "nearestOffice"                               -> null,
    "postalCode"                                  -> "9850",
    "phoneNumber"                                 -> "+45 342234 34543",
    "faxNumber"                                   -> null,
    "telexNumber"                                 -> null,
    "geoInfoCode"                                 -> null,
    "regionCode"                                  -> null,
    "traderDedicated"                             -> false,
    "dedicatedTraderLanguageCode"                 -> null,
    "dedicatedTraderName"                         -> null,
    "customsOfficeSpecificNotesCodes"             -> Json.arr("SN0009"),
    "customsOfficeLsd" -> Json.obj(
      "city"                   -> "Hirtshals",
      "languageCode"           -> "DA",
      "spaceToAdd"             -> false,
      "customsOfficeUsualName" -> "Hirtshals Toldekspedition",
      "prefixSuffixFlag"       -> false,
      "streetAndNumber"        -> "Dalsagervej 7"
    ),
    "customsOfficeTimetable" -> Json.arr(
      Json.obj(
        "seasonCode"      -> 1,
        "seasonStartDate" -> "2018-01-01",
        "seasonEndDate"   -> "2099-12-31",
        "customsOfficeTimetableLine" -> Json.arr(
          Json.obj(
            "dayInTheWeekEndDay"              -> 5,
            "openingHoursTimeFirstPeriodFrom" -> "08:00:00",
            "dayInTheWeekBeginDay"            -> 1,
            "openingHoursTimeFirstPeriodTo"   -> "16:00:00",
            "customsOfficeRoleTrafficCompetence" -> Json.arr(
              Json.obj(
                "roleName"    -> "EXL",
                "trafficType" -> "P"
              ),
              Json.obj(
                "roleName"    -> "EXL",
                "trafficType" -> "R"
              ),
              Json.obj(
                "roleName"    -> "EXP",
                "trafficType" -> "P"
              ),
              Json.obj(
                "roleName"    -> "EXP",
                "trafficType" -> "R"
              ),
              Json.obj(
                "roleName"    -> "EXT",
                "trafficType" -> "P"
              ),
              Json.obj(
                "roleName"    -> "EXT",
                "trafficType" -> "R"
              ),
              Json.obj(
                "roleName"    -> "PLA",
                "trafficType" -> "R"
              ),
              Json.obj(
                "roleName"    -> "RFC",
                "trafficType" -> "R"
              ),
              Json.obj(
                "roleName"    -> "DIS",
                "trafficType" -> "N/A"
              ),
              Json.obj(
                "roleName"    -> "IPR",
                "trafficType" -> "N/A"
              ),
              Json.obj(
                "roleName"    -> "ENQ",
                "trafficType" -> "P"
              ),
              Json.obj(
                "roleName"    -> "ENQ",
                "trafficType" -> "R"
              ),
              Json.obj(
                "roleName"    -> "ENQ",
                "trafficType" -> "N/A"
              ),
              Json.obj(
                "roleName"    -> "REC",
                "trafficType" -> "P"
              ),
              Json.obj(
                "roleName"    -> "REC",
                "trafficType" -> "R"
              ),
              Json.obj(
                "roleName"    -> "REC",
                "trafficType" -> "N/A"
              )
            )
          )
        )
      )
    )
  )

  private val testSummary =
    CustomsOfficeSummary("GB000060", "GB", "Dover", Some(Phase.NCTS6), Some(Domain.NCTS))

  private val testSummaryJson = Json.obj(
    "referenceNumber"        -> "GB000060",
    "countryCode"            -> "GB",
    "customsOfficeUsualName" -> "Dover",
    "phase"                  -> "P6",
    "domain"                 -> "NCTS"
  )

  private val pagedResultJson = Json.obj(
    "items"       -> Json.arr(testSummaryJson),
    "pageNum"     -> 1,
    "pageSize"    -> 10,
    "itemsInPage" -> 1,
    "totalItems"  -> 1,
    "totalPages"  -> 1
  )

  "fetchCustomsOfficeLists" should "return 200 OK when there are no errors" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(testOffices))

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/offices")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue
    response.json mustBe Json.arr(testOfficeJson)
  }

  it should "return 200 OK when there are no errors and phase and domain are provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(fixedInstant),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS)),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(testOfficePD))

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/offices?phase=P6&domain=NCTS")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.json mustBe Json.arr(testOfficeJson)
  }

  it should "return 400 Bad Request when there are no errors but only Phase provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/offices?phase=P6")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.json mustBe Json.obj(
      "error" -> "Both phase and domain must be provided together, or neither should be provided"
    )
    response.status mustBe Status.BAD_REQUEST
  }

  it should "return 400 Bad Request when there are no errors but only Domain provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/offices?domain=NCTS")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.json mustBe Json.obj(
      "error" -> "Both phase and domain must be provided together, or neither should be provided"
    )
    response.status mustBe Status.BAD_REQUEST
  }

  it should "return 200 OK when there are no offices to return" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(List.empty))

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/offices")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.json mustBe Json.arr()
    response.status mustBe Status.OK
  }

  it should "return 400 Bad Request when the user provides an invalid activeAt timestamp" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices?activeAt=2025-06-05")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.json mustBe Json.obj("statusCode" -> 400, "message" -> "bad request, cause: REDACTED")
    response.status mustBe Status.BAD_REQUEST
  }

  it should "parse comma-separated reference numbers, countryCodes and roles from a query parameter when there is only one country and role" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(Some(Set("IT223100"))),
        equalTo(Some(Set("GB"))),
        equalTo(Some(Set("AUT"))),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(List.empty))

    val response =
      httpClientV2
        .get(
          url"http://localhost:$port/crdl-cache/offices?referenceNumbers=IT223100&countryCodes=GB&roles=AUT"
        )
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.OK
  }

  it should "parse comma-separated reference numbers, countryCodes and roles from a query parameter when there are multiple countries and roles" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(Some(Set("IT223100", "IT223101"))),
        equalTo(Some(Set("GB", "XI"))),
        equalTo(Some(Set("AUT", "CCA"))),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(List.empty))

    val response =
      httpClientV2
        .get(
          url"http://localhost:$port/crdl-cache/offices?referenceNumbers=IT223100,IT223101&countryCodes=GB,XI&roles=AUT,CCA"
        )
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.OK
  }

  it should "parse comma-separated reference numbers, countryCodes and roles when there are multiple declarations of the query parameter" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(Some(Set("IT223100", "IT223101", "DK003102", "IT314102"))),
        equalTo(Some(Set("GB", "XI", "AW", "BL"))),
        equalTo(Some(Set("AUT", "CCA", "ACE", "RSS"))),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(List.empty))

    val response =
      httpClientV2
        .get(
          url"http://localhost:$port/crdl-cache/offices?referenceNumbers=IT223100,IT223101&referenceNumbers=DK003102,IT314102&countryCodes=GB,XI&countryCodes=AW,BL&roles=AUT,CCA&roles=ACE,RSS"
        )
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.OK
  }

  it should "parse comma-separated reference numbers, countries and roles when there is no value declared for the query parameter" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(Some(Set.empty)),
        equalTo(Some(Set.empty)),
        equalTo(Some(Set.empty)),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(List.empty))

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices?referenceNumbers=&countryCodes=&roles=")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.OK
  }

  it should "return 500 Internal Server Error when there is an error fetching from the repository" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.failed(new RuntimeException("Boom!!!")))

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.INTERNAL_SERVER_ERROR
  }

  it should "return 401 Unauthorized when the user provides no Authorization header" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices")
        .execute[HttpResponse]
        .futureValue

    response.json mustBe Json.obj("statusCode" -> 401, "message" -> "Unauthorized")
    response.status mustBe Status.UNAUTHORIZED
  }

  it should "return 401 Unauthorized when the user's token does not provide the appropriate permissions" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Unauthorized", Status.UNAUTHORIZED)))

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.json mustBe Json.obj("statusCode" -> 401, "message" -> "Unauthorized")
    response.status mustBe Status.UNAUTHORIZED
  }

  it should "return 403 Forbidden when the user's token cannot be validated" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Forbidden", Status.FORBIDDEN)))

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.json mustBe Json.obj("statusCode" -> 403, "message" -> "Forbidden")
    response.status mustBe Status.FORBIDDEN
  }

  it should "return 500 Internal Server Error when there is an error communicating with internal-auth" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Internal Server Error", 500)))

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.INTERNAL_SERVER_ERROR
  }

  it should "return 400 Bad Request when roleDate is provided without roles" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices?roleDate=2025-06-15")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.BAD_REQUEST
    response.json mustBe Json.obj(
      "statusCode" -> 400,
      "message"    -> "roleDate requires roles to be specified"
    )
  }

  it should "return 200 OK when roles and roleDate are both provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeLists(
        equalTo(None),
        equalTo(None),
        equalTo(Some(Set("TRA"))),
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(Some(LocalDate.parse("2025-06-15")))
      )
    )
      .thenReturn(Future.successful(testOffices))

    val response =
      httpClientV2
        .get(url"http://localhost:$port/crdl-cache/offices?roles=TRA&roleDate=2025-06-15")
        .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
        .execute[HttpResponse]
        .futureValue

    response.status mustBe Status.OK
  }

  "fetchCustomsOfficeDetail" should "return 200 OK with a CustomsOffice when there are no filters" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeByRef(
        equalTo(testReferenceNumber),
        equalTo(fixedInstant)
      )
    )
      .thenReturn(Future.successful(Some(testOffice)))

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
    response.json mustBe testOfficeJson
  }

  it should "return 404 NotFound when there is no matching CustomsOffice" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeByRef(
        equalTo(testReferenceNumber),
        equalTo(fixedInstant)
      )
    )
      .thenReturn(Future.successful(None))

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.NOT_FOUND
  }

  it should "return 200 OK when an activeAt timestamp is provided" in {
    val activeAt = Instant.parse("2024-01-01T00:00:00Z")
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeByRef(
        equalTo(testReferenceNumber),
        equalTo(activeAt)
      )
    )
      .thenReturn(Future.successful(Some(testOffice)))

    val response = httpClientV2
      .get(
        url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber?activeAt=${activeAt}"
      )
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
    response.json mustBe testOfficeJson
  }

  it should "return 400 Bad Request when activeAt is not a valid Instant" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val statusCode =
      try {
        httpClientV2
          .get(
            url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber?activeAt=not-a-timestamp"
          )
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
          .status
      } catch {
        case e: UpstreamErrorResponse => e.statusCode
      }

    statusCode mustBe Status.BAD_REQUEST
  }

  it should "return 500 Internal Server Error when there is an error fetching summaries from the repository" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeByRef(
        equalTo(testReferenceNumber),
        equalTo(fixedInstant)
      )
    )
      .thenReturn(Future.failed(new RuntimeException("Boom!!!")))

    val statusCode =
      try {
        httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
          .status
      } catch {
        case e: UpstreamErrorResponse => e.statusCode
      }

    statusCode mustBe Status.INTERNAL_SERVER_ERROR
  }

  it should "return 401 Unauthorized when the user provides no Authorization header" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val (statusCode, responseJson) =
      try {
        val r = httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber")
          .execute[HttpResponse]
          .futureValue
        (r.status, r.json)
      } catch {
        case e: UpstreamErrorResponse => (e.statusCode, Json.parse(e.message))
      }

    responseJson mustBe Json.obj("statusCode" -> 401, "message" -> "Unauthorized")
    statusCode mustBe Status.UNAUTHORIZED
  }

  it should "return 401 Unauthorized when the user's token does not provide the appropriate permissions" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Unauthorized", Status.UNAUTHORIZED)))

    val (statusCode, responseJson) =
      try {
        val r = httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
        (r.status, r.json)
      } catch {
        case e: UpstreamErrorResponse => (e.statusCode, Json.parse(e.message))
      }

    responseJson mustBe Json.obj("statusCode" -> 401, "message" -> "Unauthorized")
    statusCode mustBe Status.UNAUTHORIZED
  }

  it should "return 403 Forbidden when the user's token cannot be validated" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Forbidden", Status.FORBIDDEN)))

    val (statusCode, responseJson) =
      try {
        val r = httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
        (r.status, r.json)
      } catch {
        case e: UpstreamErrorResponse => (e.statusCode, Json.parse(e.message))
      }

    responseJson mustBe Json.obj("statusCode" -> 403, "message" -> "Forbidden")
    statusCode mustBe Status.FORBIDDEN
  }

  it should "return 500 Internal Server Error when there is an error communicating with internal-auth" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Internal Server Error", 500)))

    val statusCode =
      try {
        httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/$testReferenceNumber")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
          .status
      } catch {
        case e: UpstreamErrorResponse => e.statusCode
      }

    statusCode mustBe Status.INTERNAL_SERVER_ERROR
  }

  "fetchCustomsOfficeListSummaries" should "return 200 OK with a paged result when there are no filters" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Seq(testSummary)))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(1L))

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
    response.json mustBe pagedResultJson
  }

  it should "return 200 OK with an empty items list when there are no matching summaries" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Seq.empty))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(0L))

    val response = httpClientV2
      .get(url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10")
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
    response.json mustBe Json.obj(
      "items"       -> Json.arr(),
      "pageNum"     -> 1,
      "pageSize"    -> 10,
      "itemsInPage" -> 0,
      "totalItems"  -> 0,
      "totalPages"  -> 0
    )
  }

  it should "return 200 OK when an activeAt timestamp is provided" in {
    val activeAt = Instant.parse("2024-01-01T00:00:00Z")
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(activeAt),
        equalTo(1),
        equalTo(10),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Seq(testSummary)))
    when(
      repository.customsOfficesCount(
        equalTo(activeAt),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(1L))

    val response = httpClientV2
      .get(
        url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10&activeAt=2024-01-01T00:00:00Z"
      )
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
  }

  it should "return 200 OK when a referenceNumber filter is provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(Some("GB000060")),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Seq(testSummary)))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(Some("GB000060")),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(1L))

    val response = httpClientV2
      .get(
        url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10&referenceNumber=GB000060"
      )
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
  }

  it should "return 200 OK when a countryCode filter is provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(None),
        equalTo(Some("GB")),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Seq(testSummary)))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(Some("GB")),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(1L))

    val response = httpClientV2
      .get(
        url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10&countryCode=GB"
      )
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
  }

  it should "return 200 OK when an officeName filter is provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(None),
        equalTo(None),
        equalTo(Some("Dover")),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Seq(testSummary)))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(Some("Dover")),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(1L))

    val response = httpClientV2
      .get(
        url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10&officeName=Dover"
      )
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
  }

  it should "return 200 OK when an phase and domain is provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(Seq(testSummary)))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(1L))

    val response = httpClientV2
      .get(
        url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10&phase=P6&domain=NCTS"
      )
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
  }

  it should "return 200 OK when all 5 filters are provided" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(Some("GB000060")),
        equalTo(Some("GB")),
        equalTo(Some("Dover")),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(Seq(testSummary)))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(Some("GB000060")),
        equalTo(Some("GB")),
        equalTo(Some("Dover")),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(1L))

    val response = httpClientV2
      .get(
        url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10&referenceNumber=GB000060&countryCode=GB&officeName=Dover&phase=P6&domain=NCTS"
      )
      .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
      .execute[HttpResponse]
      .futureValue

    response.status mustBe Status.OK
  }

  it should "return 400 Bad Request when activeAt is not a valid Instant" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val statusCode =
      try {
        httpClientV2
          .get(
            url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10&activeAt=not-a-timestamp"
          )
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
          .status
      } catch {
        case e: UpstreamErrorResponse => e.statusCode
      }

    statusCode mustBe Status.BAD_REQUEST
  }

  it should "return 500 Internal Server Error when there is an error fetching summaries from the repository" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)
    when(
      repository.fetchCustomsOfficeSummaries(
        equalTo(fixedInstant),
        equalTo(1),
        equalTo(10),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.failed(new RuntimeException("Boom!!!")))
    when(
      repository.customsOfficesCount(
        equalTo(fixedInstant),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(0L))

    val statusCode =
      try {
        httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
          .status
      } catch {
        case e: UpstreamErrorResponse => e.statusCode
      }

    statusCode mustBe Status.INTERNAL_SERVER_ERROR
  }

  it should "return 401 Unauthorized when the user provides no Authorization header" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.unit)

    val (statusCode, responseJson) =
      try {
        val r = httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10")
          .execute[HttpResponse]
          .futureValue
        (r.status, r.json)
      } catch {
        case e: UpstreamErrorResponse => (e.statusCode, Json.parse(e.message))
      }

    responseJson mustBe Json.obj("statusCode" -> 401, "message" -> "Unauthorized")
    statusCode mustBe Status.UNAUTHORIZED
  }

  it should "return 401 Unauthorized when the user's token does not provide the appropriate permissions" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Unauthorized", Status.UNAUTHORIZED)))

    val (statusCode, responseJson) =
      try {
        val r = httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
        (r.status, r.json)
      } catch {
        case e: UpstreamErrorResponse => (e.statusCode, Json.parse(e.message))
      }

    responseJson mustBe Json.obj("statusCode" -> 401, "message" -> "Unauthorized")
    statusCode mustBe Status.UNAUTHORIZED
  }

  it should "return 403 Forbidden when the user's token cannot be validated" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Forbidden", Status.FORBIDDEN)))

    val (statusCode, responseJson) =
      try {
        val r = httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
        (r.status, r.json)
      } catch {
        case e: UpstreamErrorResponse => (e.statusCode, Json.parse(e.message))
      }

    responseJson mustBe Json.obj("statusCode" -> 403, "message" -> "Forbidden")
    statusCode mustBe Status.FORBIDDEN
  }

  it should "return 500 Internal Server Error when there is an error communicating with internal-auth" in {
    when(
      authStub.stubAuth(equalTo(Some(ReadCustomsOfficeLists)), equalTo(Retrieval.EmptyRetrieval))
    )
      .thenReturn(Future.failed(UpstreamErrorResponse("Internal Server Error", 500)))

    val statusCode =
      try {
        httpClientV2
          .get(url"http://localhost:$port/crdl-cache/admin/offices/summaries?pageNum=1&pageSize=10")
          .setHeader(HeaderNames.AUTHORIZATION -> "some-auth-token")
          .execute[HttpResponse]
          .futureValue
          .status
      } catch {
        case e: UpstreamErrorResponse => e.statusCode
      }

    statusCode mustBe Status.INTERNAL_SERVER_ERROR
  }

}
