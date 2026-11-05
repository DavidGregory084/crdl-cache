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

import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.stubbing.Scenario
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import play.api.Configuration
import play.api.http.{HeaderNames, MimeTypes}
import uk.gov.hmrc.crdlcache.config.AppConfig
import uk.gov.hmrc.crdlcache.models.CodeListCode.BC08
import uk.gov.hmrc.crdlcache.models.dps.*
import uk.gov.hmrc.crdlcache.models.dps.codelist.*
import uk.gov.hmrc.crdlcache.models.dps.col.*
import uk.gov.hmrc.crdlcache.models.{Domain, Phase}
import uk.gov.hmrc.http.test.{HttpClientV2Support, WireMockSupport}
import uk.gov.hmrc.http.{HeaderCarrier, UpstreamErrorResponse}

import java.time.{LocalDate, ZoneOffset}
import scala.concurrent.Future

class DpsConnectorSpec
  extends AsyncFlatSpec
  with Matchers
  with WireMockSupport
  with HttpClientV2Support
  with EitherValues {

  given actorSystem: ActorSystem = ActorSystem("test")

  given Materializer = Materializer(actorSystem)

  given HeaderCarrier = HeaderCarrier()

  private val clientId     = "a0ce80bc-14b6-48eb-a8c0-96f1a927a573"
  private val clientSecret = "5a8e64bc855c4e6445cab63cee753bc1"

  // Basic auth header, produced by:
  // printf "$CLIENT_ID:$CLIENT_SECRET" | base64
  private val expectedEncodedAuthHeader =
    "Basic YTBjZTgwYmMtMTRiNi00OGViLWE4YzAtOTZmMWE5MjdhNTczOjVhOGU2NGJjODU1YzRlNjQ0NWNhYjYzY2VlNzUzYmMx"

  private val config = AppConfig(
    Configuration(
      "appName"                                            -> "crdl-cache",
      "microservice.services.dps-api.host"                 -> "localhost",
      "microservice.services.dps-api.ref-data-path"        -> "iv_crdl_reference_data",
      "microservice.services.dps-api.customs-offices-path" -> "iv_crdl_customs_office",
      "microservice.services.dps-api.port"                 -> wireMockPort,
      "microservice.services.dps-api.clientId"             -> clientId,
      "microservice.services.dps-api.clientSecret"         -> clientSecret,
      "last-updated-date.default"                          -> "2025-05-29",
      "customsOfficesPageSize"                             -> "10",
      "import-codelists.schedule"                          -> "* * * * * ?",
      "import-pd-lists.schedule"                           -> "* * * * * ?",
      "import-offices.schedule"                            -> "* * * * * ?",
      "import-correspondence-lists.schedule"               -> "* * * * * ?",
      "import-offices.phase"                               -> "P6",
      "import-offices.domain"                              -> "NCTS",
      "import-codelists.codelists"                         -> List.empty,
      "import-pd-lists.pd-lists"                           -> List.empty,
      "import-correspondence-lists.correspondence-lists"   -> List.empty,
      "http-verbs.retries.intervals"                       -> List("1.millis")
    )
  )

  private val phase: Option[Phase]   = Some(Phase.NCTS6)
  private val domain: Option[Domain] = Some(Domain.NCTS)

  private val connector =
    new DpsConnector(
      httpClientV2,
      config
    )

  private val snapshotsPage1 = CodeListResponse(
    List(
      DpsCodeListSnapshot(
        BC08,
        "Country",
        1,
        List(
          DpsCodeListEntry(
            List(
              DataItem("CountryCode", Some("BL")),
              DataItem("Action_Operation", Some("U")),
              DataItem("Action_ActivationDate", Some("17-01-2024")),
              DataItem("Action_ActionIdentification", Some("823")),
              DataItem("Action_ResponsibleDataManager", None),
              DataItem("Action_ModificationDateAndTime", Some("17-01-2024"))
            ),
            List(LanguageDescription("en", "Saint Barthélemy"))
          ),
          DpsCodeListEntry(
            List(
              DataItem("CountryCode", Some("BM")),
              DataItem("Action_Operation", Some("U")),
              DataItem("Action_ActivationDate", Some("17-01-2024")),
              DataItem("Action_ActionIdentification", Some("824")),
              DataItem("Action_ResponsibleDataManager", None),
              DataItem("Action_ModificationDateAndTime", Some("17-01-2024"))
            ),
            List(LanguageDescription("en", "Bermuda"))
          )
        )
      )
    )
  )

  private val snapshotsPage2 = CodeListResponse(
    List(
      DpsCodeListSnapshot(
        BC08,
        "Country",
        12,
        List(
          DpsCodeListEntry(
            List(
              DataItem("CountryCode", Some("CX")),
              DataItem("Action_Operation", Some("U")),
              DataItem("Action_ActivationDate", Some("17-01-2024")),
              DataItem("Action_ActionIdentification", Some("848")),
              DataItem("Action_ResponsibleDataManager", None),
              DataItem("Action_ModificationDateAndTime", Some("17-01-2024"))
            ),
            List(LanguageDescription("en", "Christmas Island"))
          ),
          DpsCodeListEntry(
            List(
              DataItem("CountryCode", Some("CY")),
              DataItem("Action_Operation", Some("U")),
              DataItem("Action_ActivationDate", Some("17-01-2024")),
              DataItem("Action_ActionIdentification", Some("849")),
              DataItem("Action_ResponsibleDataManager", None),
              DataItem("Action_ModificationDateAndTime", Some("17-01-2024"))
            ),
            List(LanguageDescription("en", "Cyprus"))
          )
        )
      )
    )
  )

  private val customsOfficeListPage1 = CustomsOfficeListResponse(
    List(
      DpsCustomsOffice(
        RDEntryStatus("valid", "01-05-2025"),
        "IT223100",
        None,
        Some("ITP00002"),
        None,
        None,
        None,
        "IT",
        Some("test@test.it"),
        None,
        Some("20250501"),
        None,
        "40121",
        Some("0039 435345"),
        Some("0039 435345"),
        None,
        Some("Q"),
        None,
        "0",
        Some("IT"),
        Some("TIN"),
        List.empty,
        List(
          DpsCustomsOfficeDetail(
            "EMILIA 1 BOLOGNA",
            "IT",
            "BOLOGNA",
            "0",
            Some("A"),
            None,
            "1",
            "VIALE PIETRAMELLARA, 1/2"
          )
        ),
        List(
          DpsCustomsOfficeTimetable(
            "1",
            Some("ALL YEAR"),
            "20180101",
            "20991231",
            List(
              DpsTimetableLine(
                Some("1"),
                Some("0800"),
                Some("1800"),
                Some("6"),
                None,
                None,
                Some(
                  List(
                    DpsRoleTrafficCompetence("EXC", "R"),
                    DpsRoleTrafficCompetence("REG", "N/A"),
                    DpsRoleTrafficCompetence("SCO", "N/A"),
                    DpsRoleTrafficCompetence("PLA", "N/A"),
                    DpsRoleTrafficCompetence("DIS", "N/A"),
                    DpsRoleTrafficCompetence("RFC", "N/A"),
                    DpsRoleTrafficCompetence("EXT", "N/A"),
                    DpsRoleTrafficCompetence("EXP", "N/A"),
                    DpsRoleTrafficCompetence("IPR", "N/A")
                  )
                )
              )
            )
          )
        )
      ),
      DpsCustomsOffice(
        RDEntryStatus("valid", "01-05-2025"),
        "IT223101",
        None,
        Some("ITP00002"),
        Some("IT223101"),
        Some("IT223101"),
        None,
        "IT",
        Some("test@it"),
        None,
        Some("20250501"),
        None,
        "40131",
        Some("1234 045483382"),
        Some("2343 34543"),
        None,
        Some("Q"),
        None,
        "0",
        Some("IT"),
        Some("TIN"),
        List.empty,
        List(
          DpsCustomsOfficeDetail(
            "AEROPORTO DI BOLOGNA",
            "IT",
            "BOLOGNA",
            "0",
            Some("A"),
            None,
            "1",
            "VIA DELL'AEROPORTO, 1"
          )
        ),
        List(
          DpsCustomsOfficeTimetable(
            "1",
            Some("ALL YEAR"),
            "20180101",
            "20991231",
            List(
              DpsTimetableLine(
                Some("1"),
                Some("0000"),
                Some("2359"),
                Some("6"),
                None,
                None,
                Some(
                  List(
                    DpsRoleTrafficCompetence("DEP", "AIR"),
                    DpsRoleTrafficCompetence("INC", "AIR"),
                    DpsRoleTrafficCompetence("TXT", "AIR"),
                    DpsRoleTrafficCompetence("DES", "AIR"),
                    DpsRoleTrafficCompetence("ENQ", "N/A"),
                    DpsRoleTrafficCompetence("ENT", "AIR"),
                    DpsRoleTrafficCompetence("EXC", "N/A"),
                    DpsRoleTrafficCompetence("EXP", "AIR"),
                    DpsRoleTrafficCompetence("EXT", "AIR"),
                    DpsRoleTrafficCompetence("REC", "N/A"),
                    DpsRoleTrafficCompetence("REG", "N/A"),
                    DpsRoleTrafficCompetence("TRA", "AIR"),
                    DpsRoleTrafficCompetence("EIN", "AIR"),
                    DpsRoleTrafficCompetence("PLA", "N/A"),
                    DpsRoleTrafficCompetence("DIS", "N/A"),
                    DpsRoleTrafficCompetence("RFC", "N/A"),
                    DpsRoleTrafficCompetence("IPR", "N/A")
                  )
                )
              )
            )
          )
        )
      )
    )
  )

  private val customsOfficeListPage2 = CustomsOfficeListResponse(
    List(
      DpsCustomsOffice(
        RDEntryStatus("valid", "22-03-2025"),
        "DK003102",
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
        "0",
        None,
        None,
        List(SpecificNotes("SN0009")),
        List(
          DpsCustomsOfficeDetail(
            "Hirtshals Toldekspedition",
            "DA",
            "Hirtshals",
            "0",
            None,
            None,
            "0",
            "Dalsagervej 7"
          )
        ),
        List(
          DpsCustomsOfficeTimetable(
            "1",
            None,
            "20180101",
            "20991231",
            List(
              DpsTimetableLine(
                Some("1"),
                Some("0800"),
                Some("1600"),
                Some("5"),
                None,
                None,
                Some(
                  List(
                    DpsRoleTrafficCompetence("EXL", "P"),
                    DpsRoleTrafficCompetence("EXL", "R"),
                    DpsRoleTrafficCompetence("EXP", "P"),
                    DpsRoleTrafficCompetence("EXP", "R"),
                    DpsRoleTrafficCompetence("EXT", "P"),
                    DpsRoleTrafficCompetence("EXT", "R"),
                    DpsRoleTrafficCompetence("PLA", "R"),
                    DpsRoleTrafficCompetence("RFC", "R"),
                    DpsRoleTrafficCompetence("DIS", "N/A"),
                    DpsRoleTrafficCompetence("IPR", "N/A"),
                    DpsRoleTrafficCompetence("ENQ", "P"),
                    DpsRoleTrafficCompetence("ENQ", "R"),
                    DpsRoleTrafficCompetence("ENQ", "N/A"),
                    DpsRoleTrafficCompetence("REC", "P"),
                    DpsRoleTrafficCompetence("REC", "R"),
                    DpsRoleTrafficCompetence("REC", "N/A")
                  )
                )
              )
            )
          )
        )
      ),
      DpsCustomsOffice(
        RDEntryStatus("valid", "22-03-2025"),
        "IT314102",
        None,
        Some("ITP00023"),
        Some("IT314102"),
        Some("IT314102"),
        None,
        "IT",
        Some("testo@it"),
        None,
        None,
        None,
        "10043",
        Some("345 34234"),
        None,
        None,
        None,
        None,
        "0",
        Some("IT"),
        Some("TIN"),
        List.empty,
        List(
          DpsCustomsOfficeDetail(
            "ORBASSANO",
            "IT",
            "ORBASSANO (TO)",
            "0",
            Some("A"),
            None,
            "1",
            "Prima Strada, 5"
          )
        ),
        List(
          DpsCustomsOfficeTimetable(
            "1",
            Some("ALL YEAR"),
            "20240101",
            "99991231",
            List(
              DpsTimetableLine(
                Some("1"),
                Some("0800"),
                Some("1800"),
                Some("5"),
                None,
                None,
                Some(
                  List(
                    DpsRoleTrafficCompetence("DEP", "R"),
                    DpsRoleTrafficCompetence("INC", "R"),
                    DpsRoleTrafficCompetence("TRA", "R"),
                    DpsRoleTrafficCompetence("EXP", "R"),
                    DpsRoleTrafficCompetence("EIN", "R"),
                    DpsRoleTrafficCompetence("ENT", "R"),
                    DpsRoleTrafficCompetence("EXC", "R"),
                    DpsRoleTrafficCompetence("DES", "R"),
                    DpsRoleTrafficCompetence("GUA", "R"),
                    DpsRoleTrafficCompetence("EXT", "R"),
                    DpsRoleTrafficCompetence("REG", "R"),
                    DpsRoleTrafficCompetence("REC", "R"),
                    DpsRoleTrafficCompetence("IPR", "N/A"),
                    DpsRoleTrafficCompetence("ENQ", "N/A")
                  )
                )
              )
            )
          )
        )
      )
    )
  )

  override lazy val wireMockRootDirectory = "it/test/resources"
  private val uuidRegex = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

  "DPSConnector.fetchCodelistSnapshots" should "produce a CodeListResponse for each page of codelist snapshots" in {
    val lastUpdatedDate = LocalDate.of(2025, 5, 28).atStartOfDay(ZoneOffset.UTC).toInstant

    // Page 1
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page1.json")
        )
    )

    // Page 2
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("10"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page2.json")
        )
    )

    // Page 3 (empty)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("20"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page3.json")
        )
    )

    connector
      .fetchCodeListSnapshots(BC08, lastUpdatedDate)
      .runWith(Sink.collection[CodeListResponse, List[CodeListResponse]])
      .map(_ mustBe List(snapshotsPage1, snapshotsPage2))
  }

  it should "throw UpstreamErrorResponse when there is a client error in the first page" in {
    val lastUpdatedDate = LocalDate.of(2025, 5, 28).atStartOfDay(ZoneOffset.UTC).toInstant

    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          badRequest().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCodeListSnapshots(BC08, lastUpdatedDate)
        .runWith(Sink.collection[CodeListResponse, List[CodeListResponse]])
    }
  }

  it should "throw UpstreamErrorResponse when there is a server error in the first page" in {
    val lastUpdatedDate = LocalDate.of(2025, 5, 28).atStartOfDay(ZoneOffset.UTC).toInstant

    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          serverError().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCodeListSnapshots(BC08, lastUpdatedDate)
        .runWith(Sink.collection[CodeListResponse, List[CodeListResponse]])
    }
  }

  it should "throw UpstreamErrorResponse when there is a client error in the second page" in {
    val lastUpdatedDate = LocalDate.of(2025, 5, 28).atStartOfDay(ZoneOffset.UTC).toInstant

    // Page 1
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page1.json")
        )
    )

    // Page 2 (Bad Request)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("10"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          badRequest().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCodeListSnapshots(BC08, lastUpdatedDate)
        .runWith(Sink.collection[CodeListResponse, List[CodeListResponse]])
    }
  }

  it should "throw UpstreamErrorResponse when there is a server error in the second page" in {
    val lastUpdatedDate = LocalDate.of(2025, 5, 28).atStartOfDay(ZoneOffset.UTC).toInstant

    // Page 1
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page1.json")
        )
    )

    // Page 2 (Internal Server Error)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("10"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          serverError().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCodeListSnapshots(BC08, lastUpdatedDate)
        .runWith(Sink.collection[CodeListResponse, List[CodeListResponse]])
    }
  }

  it should "not retry when there is a client error while fetching a page" in {
    val retryScenario   = "Retry"
    val failedState     = "Failed"
    val lastUpdatedDate = LocalDate.of(2025, 5, 28).atStartOfDay(ZoneOffset.UTC).toInstant

    // Page 1 (Bad Request)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(Scenario.STARTED)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          badRequest().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
        .willSetStateTo(failedState)
    )

    // Page 1 (Retry, OK) - it would succeed if it did retry, but it shouldn't do that!
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(failedState)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page1.json")
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCodeListSnapshots(BC08, lastUpdatedDate)
        .runWith(Sink.collection[CodeListResponse, List[CodeListResponse]])
    }
  }

  it should "retry when there is a server error while fetching a page" in {
    val retryScenario   = "Retry"
    val failedState     = "Failed"
    val lastUpdatedDate = LocalDate.of(2025, 5, 28).atStartOfDay(ZoneOffset.UTC).toInstant

    // Page 1 (Internal Server Error)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(Scenario.STARTED)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          serverError().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
        .willSetStateTo(failedState)
    )

    // Page 1 (Retry, OK)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(failedState)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page1.json")
        )
    )

    // Page 2
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("10"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page2.json")
        )
    )

    // Page 3 (empty)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_reference_data"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("code_list_code", equalTo("BC08"))
        .withQueryParam("last_updated_date", equalTo("2025-05-28T00:00:00Z"))
        .withQueryParam("$start_index", equalTo("20"))
        .withQueryParam("$count", equalTo("10"))
        .withQueryParam("$orderby", equalTo("snapshotversion ASC"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("codelist/BC08_snapshots_page3.json")
        )
    )

    connector
      .fetchCodeListSnapshots(BC08, lastUpdatedDate)
      .runWith(Sink.collection[CodeListResponse, List[CodeListResponse]])
      .map(_ mustBe List(snapshotsPage1, snapshotsPage2))
  }

  "DPSConnector.fetchCustomsOfficeLists" should "produce a CustomOfficeListResponse for each page of custom office list" in {
    // Page 1
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page1.json")
        )
    )

    // Page 2
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("10"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page2.json")
        )
    )

    // Page 3 (empty)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("20"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page3.json")
        )
    )

    connector
      .fetchCustomsOfficeLists(phase, domain)
      .runWith(Sink.collection[CustomsOfficeListResponse, List[CustomsOfficeListResponse]])
      .map(_ mustBe List(customsOfficeListPage1, customsOfficeListPage2))
  }

  it should "throw UpstreamErrorResponse when there is a client error in the first page" in {
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          badRequest().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCustomsOfficeLists(phase, domain)
        .runWith(Sink.collection[CustomsOfficeListResponse, List[CustomsOfficeListResponse]])
    }
  }

  it should "throw UpstreamErrorResponse when there is a server error in the first page" in {
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          serverError().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCustomsOfficeLists(phase, domain)
        .runWith(Sink.collection[CustomsOfficeListResponse, List[CustomsOfficeListResponse]])
    }
  }

  it should "throw UpstreamErrorResponse when there is a client error in the second page" in {
    // Page 1
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page1.json")
        )
    )

    // Page 2 (Bad Request)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withQueryParam("$start_index", equalTo("10"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          badRequest().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCustomsOfficeLists(phase, domain)
        .runWith(Sink.collection[CustomsOfficeListResponse, List[CustomsOfficeListResponse]])
    }
  }

  it should "throw UpstreamErrorResponse when there is a server error in the second page" in {
    // Page 1
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page1.json")
        )
    )

    // Page 2 (Internal Server Error)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          serverError().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCustomsOfficeLists(phase, domain)
        .runWith(Sink.collection[CustomsOfficeListResponse, List[CustomsOfficeListResponse]])
    }
  }

  it should "not retry when there is a client error while fetching a page" in {
    val retryScenario = "Retry"
    val failedState   = "Failed"

    // Page 1 (Bad Request)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(Scenario.STARTED)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          badRequest().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
        .willSetStateTo(failedState)
    )

    // Page 1 (Retry, OK) - it would succeed if it did retry, but it shouldn't do that!
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(failedState)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page1.json")
        )
    )

    recoverToSucceededIf[UpstreamErrorResponse] {
      connector
        .fetchCustomsOfficeLists(phase, domain)
        .runWith(Sink.collection[CustomsOfficeListResponse, List[CustomsOfficeListResponse]])
    }
  }

  it should "retry when there is a server error while fetching a page" in {
    val retryScenario = "Retry"
    val failedState   = "Failed"

    // Page 1 (Internal Server Error)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(Scenario.STARTED)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          serverError().withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
        )
        .willSetStateTo(failedState)
    )

    // Page 1 (Retry, OK)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .inScenario(retryScenario)
        .whenScenarioStateIs(failedState)
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("0"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page1.json")
        )
    )

    // Page 2
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("10"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page2.json")
        )
    )

    // Page 3 (empty)
    stubFor(
      get(urlPathEqualTo("/iv_crdl_customs_office"))
        .withHeader("correlationId", matching(uuidRegex))
        .withHeader(HeaderNames.AUTHORIZATION, equalTo(expectedEncodedAuthHeader))
        .withQueryParam("$start_index", equalTo("20"))
        .withQueryParam("$count", equalTo("10"))
        .willReturn(
          ok()
            .withHeader(HeaderNames.CONTENT_TYPE, MimeTypes.JSON)
            .withBodyFile("col/COL_page3.json")
        )
    )

    connector
      .fetchCustomsOfficeLists(phase, domain)
      .runWith(Sink.collection[CustomsOfficeListResponse, List[CustomsOfficeListResponse]])
      .map(_ mustBe List(customsOfficeListPage1, customsOfficeListPage2))
  }
}
