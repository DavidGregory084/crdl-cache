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
import org.mongodb.scala.model.{Filters, Sorts}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Assertion, OptionValues}
import uk.gov.hmrc.crdlcache.models.*
import uk.gov.hmrc.crdlcache.models.CustomsOfficeListsInstruction.{
  RecordMissingCustomsOffice,
  UpsertCustomsOffice
}
import uk.gov.hmrc.mongo.test.{
  CleanMongoCollectionSupport,
  IndexedMongoQueriesSupport,
  PlayMongoRepositorySupport
}
import uk.gov.hmrc.mongo.transaction.TransactionConfiguration

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, Instant, LocalDate, LocalTime}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class CustomsOfficeListsRepositorySpec
  extends AnyFlatSpec
  with PlayMongoRepositorySupport[CustomsOffice]
  with CleanMongoCollectionSupport
  with IndexedMongoQueriesSupport
  with Matchers
  with OptionValues
  with ScalaFutures {

  given TransactionConfiguration = TransactionConfiguration.strict

  given ec: ExecutionContext = ExecutionContext.global

  override protected val repository: CustomsOfficeListsRepository =
    new CustomsOfficeListsRepository(mongoComponent)

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 30.seconds, interval = 100.millis)

  def withCustomsOfficeEntries(
    offices: Seq[CustomsOffice]
  )(test: ClientSession => Future[Assertion]): Unit = {
    repository.collection.insertMany(offices).toFuture.futureValue
    repository.withSessionAndTransaction(test).futureValue
  }

  protected val timeFormat: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_TIME
  private val dateFormat                      = DateTimeFormatter.ofPattern("yyyyMMdd")

  val DK003102 = CustomsOffice(
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
    None,
    None
  )

  val DK003103_P6D = CustomsOffice(
    "DK003103",
    Instant.parse("2025-03-22T00:00:00Z"),
    None,
    None,
    None,
    Some("DK003103"),
    Some("DK003103"),
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

  val DK003104_P5D = CustomsOffice(
    "DK003104",
    Instant.parse("2025-03-22T00:00:00Z"),
    None,
    None,
    None,
    Some("DK003104"),
    Some("DK003104"),
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
    Some(Phase.NCTS5),
    Some(Domain.NCTS)
  )

  val DK003105_P6D = CustomsOffice(
    "DK003105",
    Instant.parse("2025-03-22T00:00:00Z"),
    None,
    None,
    None,
    Some("DK003105"),
    Some("DK003105"),
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
              RoleTrafficCompetence("RFC", "R"),
              RoleTrafficCompetence("DIS", "N/A"),
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

  val IT223102_P6D = CustomsOffice(
    "IT223102",
    Instant.parse("2025-03-22T00:00:00Z"),
    None,
    None,
    None,
    Some("IT223102"),
    Some("IT223102"),
    None,
    "IT",
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
      "AEROPORTO DI BOLOGNA",
      "IT",
      "BOLOGNA",
      false,
      Some("A"),
      None,
      true,
      "VIA DELL'AEROPORTO, 1"
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

  val invalidatedoffice = CustomsOffice(
    "IT314102",
    Instant.parse("2025-03-22T00:00:00Z"),
    Some(Instant.parse("2025-05-22T00:00:00Z")),
    Some("ITP00023"),
    Some("IT314102"),
    Some("IT314102"),
    None,
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
    false,
    Some("IT"),
    Some("TIN"),
    List.empty,
    CustomsOfficeDetail(
      "ORBASSANO",
      "IT",
      "ORBASSANO (TO)",
      false,
      Some("A"),
      None,
      true,
      "Prima Strada, 5"
    ),
    List(
      CustomsOfficeTimetable(
        1,
        Some("ALL YEAR"),
        LocalDate.parse("20180101", dateFormat),
        LocalDate.parse("99991231", dateFormat),
        List(
          TimetableLine(
            DayOfWeek.of(1),
            LocalTime.parse("08:00", timeFormat),
            LocalTime.parse("18:00", timeFormat),
            DayOfWeek.of(5),
            None,
            None,
            List(
              RoleTrafficCompetence("DEP", "R"),
              RoleTrafficCompetence("INC", "R"),
              RoleTrafficCompetence("TRA", "R"),
              RoleTrafficCompetence("EXP", "R"),
              RoleTrafficCompetence("EIN", "R"),
              RoleTrafficCompetence("ENT", "R"),
              RoleTrafficCompetence("EXC", "R"),
              RoleTrafficCompetence("DES", "R"),
              RoleTrafficCompetence("GUA", "R"),
              RoleTrafficCompetence("EXT", "R"),
              RoleTrafficCompetence("REG", "R"),
              RoleTrafficCompetence("REC", "R"),
              RoleTrafficCompetence("IPR", "N/A"),
              RoleTrafficCompetence("ENQ", "N/A")
            )
          )
        )
      )
    ),
    None,
    None
  )

  val newOffice = CustomsOffice(
    "IT223101",
    Instant.parse("2025-05-01T00:00:00Z"),
    None,
    Some("ITP00002"),
    Some("IT223101"),
    Some("IT223101"),
    None,
    None,
    "IT",
    Some("test@it"),
    None,
    Some(LocalDate.parse("20250501", dateFormat)),
    None,
    "40131",
    Some("1234 045483382"),
    Some("2343 34543"),
    None,
    Some("Q"),
    None,
    false,
    Some("IT"),
    Some("TIN"),
    List.empty,
    CustomsOfficeDetail(
      "AEROPORTO DI BOLOGNA",
      "IT",
      "BOLOGNA",
      false,
      Some("A"),
      None,
      true,
      "VIA DELL'AEROPORTO, 1"
    ),
    List(
      CustomsOfficeTimetable(
        1,
        Some("ALL YEAR"),
        LocalDate.parse("20180101", dateFormat),
        LocalDate.parse("20991231", dateFormat),
        List(
          TimetableLine(
            DayOfWeek.of(1),
            LocalTime.parse("00:00", timeFormat),
            LocalTime.parse("23:59", timeFormat),
            DayOfWeek.of(6),
            None,
            None,
            List(
              RoleTrafficCompetence("DEP", "AIR"),
              RoleTrafficCompetence("INC", "AIR"),
              RoleTrafficCompetence("TXT", "AIR"),
              RoleTrafficCompetence("DES", "AIR"),
              RoleTrafficCompetence("ENQ", "N/A"),
              RoleTrafficCompetence("ENT", "AIR"),
              RoleTrafficCompetence("EXC", "N/A"),
              RoleTrafficCompetence("EXP", "AIR"),
              RoleTrafficCompetence("EXT", "AIR"),
              RoleTrafficCompetence("REC", "N/A"),
              RoleTrafficCompetence("REG", "N/A"),
              RoleTrafficCompetence("TRA", "AIR"),
              RoleTrafficCompetence("EIN", "AIR"),
              RoleTrafficCompetence("PLA", "N/A"),
              RoleTrafficCompetence("DIS", "N/A"),
              RoleTrafficCompetence("RFC", "N/A"),
              RoleTrafficCompetence("IPR", "N/A")
            )
          )
        )
      )
    ),
    None,
    None
  )

  val postDatedOffice = newOffice.copy(activeFrom = Instant.parse("2026-05-01T00:00:00Z"))

  val officeWithACERole = newOffice.copy(
    customsOfficeTimetable = List(
      CustomsOfficeTimetable(
        1,
        Some("ALL YEAR"),
        LocalDate.parse("20180101", dateFormat),
        LocalDate.parse("20991231", dateFormat),
        List(
          TimetableLine(
            DayOfWeek.of(1),
            LocalTime.parse("00:00", timeFormat),
            LocalTime.parse("23:59", timeFormat),
            DayOfWeek.of(6),
            None,
            None,
            List(
              RoleTrafficCompetence("ACE", "AIR")
            )
          )
        )
      )
    )
  )

  private val customsOffices =
    Seq(
      DK003102,
      DK003103_P6D,
      DK003104_P5D,
      DK003105_P6D,
      IT223102_P6D,
      invalidatedoffice,
      postDatedOffice
    )

  // Test date for roleDate filtering: 2026-01-01
  // Test role: TRA
  private val testDate = LocalDate.parse("20260101", dateFormat)
  private val testActiveAt = Instant.parse("2025-06-05T00:00:00Z")

  private def makeTimetable(seasonStart: String, seasonEnd: String, roles: List[String]): CustomsOfficeTimetable =
    CustomsOfficeTimetable(
      1,
      Some("SEASON"),
      LocalDate.parse(seasonStart, dateFormat),
      LocalDate.parse(seasonEnd, dateFormat),
      List(
        TimetableLine(
          DayOfWeek.of(1),
          LocalTime.parse("08:00", timeFormat),
          LocalTime.parse("18:00", timeFormat),
          DayOfWeek.of(5),
          None,
          None,
          roles.map(RoleTrafficCompetence(_, "R"))
        )
      )
    )

  // 1. Single valid timetable — role matches, season includes test date
  val singleValidOffice = newOffice.copy(
    referenceNumber = "DE000001",
    countryCode = "DE",
    customsOfficeTimetable = List(makeTimetable("20250101", "20261231", List("TRA")))
  )

  // 2. Single invalid timetable — past — role matches, season ends before test date
  val singleInvalidPastOffice = newOffice.copy(
    referenceNumber = "DE000002",
    countryCode = "DE",
    customsOfficeTimetable = List(makeTimetable("20240101", "20251231", List("TRA")))
  )

  // 3. Single invalid timetable — future — role matches, season starts after test date
  val singleInvalidFutureOffice = newOffice.copy(
    referenceNumber = "DE000003",
    countryCode = "DE",
    customsOfficeTimetable = List(makeTimetable("20260201", "20261231", List("TRA")))
  )

  // 4. Single invalid timetable — no role — season includes test date but role does not match
  val singleInvalidNoRoleOffice = newOffice.copy(
    referenceNumber = "DE000004",
    countryCode = "DE",
    customsOfficeTimetable = List(makeTimetable("20250101", "20261231", List("DEP")))
  )

  // 5. Multiple timetables — both valid — both contain role, both season includes test date
  val multipleBothValidOffice = newOffice.copy(
    referenceNumber = "DE000005",
    countryCode = "DE",
    customsOfficeTimetable = List(
      makeTimetable("20250101", "20261231", List("TRA")),
      makeTimetable("20250601", "20260601", List("TRA"))
    )
  )

  // 6. Multiple timetables — one valid/one invalid past
  val multipleOneValidOnePastOffice = newOffice.copy(
    referenceNumber = "DE000006",
    countryCode = "DE",
    customsOfficeTimetable = List(
      makeTimetable("20250101", "20261231", List("TRA")),
      makeTimetable("20240101", "20251231", List("TRA"))
    )
  )

  // 7. Multiple timetables — one valid/one invalid future
  val multipleOneValidOneFutureOffice = newOffice.copy(
    referenceNumber = "DE000007",
    countryCode = "DE",
    customsOfficeTimetable = List(
      makeTimetable("20250101", "20261231", List("TRA")),
      makeTimetable("20260201", "20261231", List("TRA"))
    )
  )

  // 8. Multiple timetables — one valid/one invalid no role
  val multipleOneValidOneNoRoleOffice = newOffice.copy(
    referenceNumber = "DE000008",
    countryCode = "DE",
    customsOfficeTimetable = List(
      makeTimetable("20250101", "20261231", List("TRA")),
      makeTimetable("20250101", "20261231", List("DEP"))
    )
  )

  // 9. Multiple timetables — both invalid — past and future
  val multipleBothInvalidPastFutureOffice = newOffice.copy(
    referenceNumber = "DE000009",
    countryCode = "DE",
    customsOfficeTimetable = List(
      makeTimetable("20240101", "20251231", List("TRA")),
      makeTimetable("20260201", "20261231", List("TRA"))
    )
  )

  // 10. Multiple timetables — both invalid — no roles
  val multipleBothInvalidNoRolesOffice = newOffice.copy(
    referenceNumber = "DE000010",
    countryCode = "DE",
    customsOfficeTimetable = List(
      makeTimetable("20250101", "20261231", List("DEP")),
      makeTimetable("20250101", "20261231", List("EXP"))
    )
  )

  val defaultActiveAt = Instant.parse("2025-06-05T00:00:00Z")

  "CustomsOfficeListsRepository.fetchCustomsOfficeReferenceNumbers" should "return active offices present in the database" in withCustomsOfficeEntries(
    customsOffices
  ) { session =>
    repository.fetchCustomsOfficeReferenceNumbers(session).map(_ must contain("DK003102"))
  }

  it should "not return entries that are invalidated" in withCustomsOfficeEntries(customsOffices) {
    session =>
      repository.fetchCustomsOfficeReferenceNumbers(session).map(_ mustNot contain("IT314102"))
  }

  "CustomsOfficeListsRepository.executeInstructions" should "invalidate missing entries" in withCustomsOfficeEntries(
    customsOffices
  ) { session =>
    for {
      _ <- repository.executeInstructions(
        session,
        List(RecordMissingCustomsOffice("DK003102", Instant.parse("2025-05-22T00:00:00Z")))
      )
      office <- repository.collection
        .find(session, Filters.equal("referenceNumber", "DK003102"))
        .toFuture()
    } yield office mustBe Seq(DK003102.copy(activeTo = Some(Instant.parse("2025-05-22T00:00:00Z"))))
  }

  it should "invalidate missing office when the activation date is the same as the existing one" in withCustomsOfficeEntries(
    customsOffices
  ) { session =>
    for {
      _ <- repository.executeInstructions(
        session,
        List(RecordMissingCustomsOffice("DK003102", DK003102.activeFrom))
      )
      office <- repository.collection
        .find(session, Filters.equal("referenceNumber", "DK003102"))
        .toFuture()
    } yield office mustBe Seq(DK003102.copy(activeTo = Some(DK003102.activeFrom)))
  }

  it should "supersede and invalidate existing entries" in withCustomsOfficeEntries(
    customsOffices
  ) { session =>
    for {
      _ <- repository.executeInstructions(
        session,
        List(
          UpsertCustomsOffice(
            DK003102.copy(
              activeFrom = Instant.parse("2025-04-22T00:00:00Z"),
              emailAddress = Some("newEmail@test")
            )
          )
        )
      )
      offices <- repository.collection
        .find(session, Filters.equal("referenceNumber", "DK003102"))
        .sort(Sorts.ascending("activeFrom"))
        .toFuture()
    } yield offices mustBe Seq(
      DK003102.copy(activeTo = Some(Instant.parse("2025-04-22T00:00:00Z"))),
      DK003102.copy(
        activeFrom = Instant.parse("2025-04-22T00:00:00Z"),
        emailAddress = Some("newEmail@test")
      )
    )
  }

  it should "create a new office when a new entry is encountered" in withCustomsOfficeEntries(
    customsOffices
  ) { session =>
    for {
      _ <- repository.executeInstructions(
        session,
        List(UpsertCustomsOffice(newOffice), UpsertCustomsOffice(postDatedOffice))
      )
      offices <- repository.collection
        .find(session, Filters.equal("referenceNumber", "IT223101"))
        .toFuture()
    } yield offices mustBe Seq(
      newOffice.copy(activeTo = Some(Instant.parse("2026-05-01T00:00:00Z"))),
      postDatedOffice
    )
  }

  it should "replace existing offices with same active from date" in withCustomsOfficeEntries(
    customsOffices
  ) { session =>
    for {
      _ <- repository.executeInstructions(
        session,
        List(
          UpsertCustomsOffice(
            DK003102.copy(
              emailAddress = Some("newEmail@test")
            )
          )
        )
      )
      office <- repository.collection
        .find(session, Filters.equal("referenceNumber", "DK003102"))
        .toFuture()
    } yield office mustBe Seq(
      DK003102.copy(
        emailAddress = Some("newEmail@test")
      )
    )
  }

  it should "upsert entries in the order of their active from date" in {
    repository.withSessionAndTransaction { session =>
      for {
        _ <- repository.executeInstructions(
          session,
          List(
            UpsertCustomsOffice(
              DK003102.copy(
                activeFrom = Instant.parse("2025-05-23T00:00:00Z"),
                emailAddress = Some("newEmail@test")
              )
            ),
            RecordMissingCustomsOffice("DK003102", Instant.parse("2025-05-22T00:00:00Z")),
            UpsertCustomsOffice(DK003102),
            UpsertCustomsOffice(newOffice)
          )
        )
        offices <- repository.collection
          .find(session)
          .sort(Sorts.ascending("activeFrom"))
          .toFuture()
      } yield offices mustBe Seq(
        DK003102.copy(activeTo = Some(Instant.parse("2025-05-22T00:00:00Z"))),
        newOffice,
        DK003102.copy(
          activeFrom = Instant.parse("2025-05-23T00:00:00Z"),
          emailAddress = Some("newEmail@test")
        )
      )
    }.futureValue
  }

  "CustomsOfficeLists.fetchCustomsOfficeLists" should "return the customs office list whose activeFrom date is before the requested date" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ must contain(DK003102))
  }

  it should "not return offices that have been superseded" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ mustNot contain(invalidatedoffice))
  }

  it should "not return offices that are not yet active" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ mustNot contain(postDatedOffice))
  }

  it should "return offices that have been invalidated if the invalidation date is in the future" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-04-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ must contain(invalidatedoffice))
  }

  it should "apply filtering of offices according to the supplied referenceNumbers" in withCustomsOfficeEntries(
    customsOffices :+ newOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = Some(Set("IT223101")),
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ mustBe List(newOffice))
  }

  it should "apply filtering of offices according to the supplied countryCodes" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = Some(Set("DK")),
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ must contain(DK003102))
  }

  it should "return only return entries matching matching phase and domain when both are provided" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = Some(Phase.NCTS6),
        domain = Some(Domain.NCTS)
      )
      .map { result =>
        result must contain allOf (DK003103_P6D, DK003105_P6D, IT223102_P6D)
        result mustNot contain allOf (DK003102, DK003104_P5D)
      }
  }

  it should "it should filter by phase and domain and exclude entries with different phase" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = Some(Phase.NCTS5),
        domain = Some(Domain.NCTS)
      )
      .map { result =>
        result must contain(DK003104_P5D)
        result mustNot contain allOf (DK003102, DK003103_P6D, DK003105_P6D, IT223102_P6D)
      }
  }

  it should "only return entries without phase and domain if neither are provided" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = Some(Set("DK")),
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map { result =>
        result must contain(DK003102)
        result mustNot contain allOf (DK003103_P6D, DK003104_P5D, DK003105_P6D, IT223102_P6D)
      }
  }

  it should "combine phase/domain filter with reference number filter" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = Some(Set("DK003105")),
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = Some(Phase.NCTS6),
        domain = Some(Domain.NCTS)
      )
      .map { result =>
        result must contain(DK003105_P6D)
        result mustNot contain allOf (DK003102, DK003103_P6D, DK003104_P5D, IT223102_P6D)
      }
  }

  it should "combine phase/domain filter with country codes filter" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = Some(Set("IT")),
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = Some(Phase.NCTS6),
        domain = Some(Domain.NCTS)
      )
      .map { result =>
        result must contain(IT223102_P6D)
        result mustNot contain allOf (DK003102, DK003103_P6D, DK003104_P5D, DK003105_P6D)
      }
  }

  it should "combine phase/domain filter with roles filter" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = Some(Set("PLA")),
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = Some(Phase.NCTS6),
        domain = Some(Domain.NCTS)
      )
      .map { result =>
        result must contain allOf (DK003103_P6D, IT223102_P6D)
        result mustNot contain allOf (DK003102, DK003104_P5D, DK003105_P6D)
      }
  }

  it should "return empty if phase and domain combination do not match entries" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = Some(Set("DK")),
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = Some(Phase("P20")),
        domain = Some(Domain("INVALID"))
      )
      .map(_ mustBe empty)
  }

  it should "apply filtering of offices according to the supplied roles" in withCustomsOfficeEntries(
    customsOffices :+ officeWithACERole
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = Some(Set("EXL")),
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ mustBe List(DK003102))
  }

  it should "not apply filtering of referenceNumbers, countries and roles when the set of supplied referenceNumbers, countries and roles is empty" in withCustomsOfficeEntries(
    customsOffices :+ newOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = Some(Set.empty),
        countryCodes = Some(Set.empty),
        roles = Some(Set.empty),
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ must contain allElementsOf List(DK003102, newOffice))
  }

  it should "not return other offices even when matching referenceNumbers are specified" in withCustomsOfficeEntries(
    customsOffices :+ newOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = Some(Set("IT223101")),
        countryCodes = None,
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ mustNot contain(DK003102))
  }

  it should "not return other offices even when matching countryCodes are specified" in withCustomsOfficeEntries(
    customsOffices :+ newOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = Some(Set("IT")),
        roles = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ mustNot contain(DK003102))
  }

  it should "not return other offices even when matching roles are specified" in withCustomsOfficeEntries(
    customsOffices :+ newOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None,
        countryCodes = None,
        roles = Some(Set("EIN")),
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        phase = None,
        domain = None
      )
      .map(_ mustNot contain(DK003102))
  }

  // Scenario 1: Single valid timetable — role matches, season includes test date → Yes
  it should "return office with single valid timetable when role and roleDate match" in withCustomsOfficeEntries(
    customsOffices :+ singleValidOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ must contain(singleValidOffice))
  }

  // Scenario 2: Single invalid timetable — past — season ends before test date → No
  it should "not return office with single past timetable when roleDate is after season end" in withCustomsOfficeEntries(
    customsOffices :+ singleInvalidPastOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ mustNot contain(singleInvalidPastOffice))
  }

  // Scenario 3: Single invalid timetable — future — season starts after test date → No
  it should "not return office with single future timetable when roleDate is before season start" in withCustomsOfficeEntries(
    customsOffices :+ singleInvalidFutureOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ mustNot contain(singleInvalidFutureOffice))
  }

  // Scenario 4: Single invalid timetable — no role — season includes test date but role doesn't match → No
  it should "not return office when season includes roleDate but role does not match" in withCustomsOfficeEntries(
    customsOffices :+ singleInvalidNoRoleOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ mustNot contain(singleInvalidNoRoleOffice))
  }

  // Scenario 5: Multiple timetables — both valid — both contain role, both season includes test date → Yes
  it should "return office with multiple valid timetables both matching role and date" in withCustomsOfficeEntries(
    customsOffices :+ multipleBothValidOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ must contain(multipleBothValidOffice))
  }

  // Scenario 6: Multiple timetables — one valid/one invalid past → Yes
  it should "return office when one timetable is valid and another is past" in withCustomsOfficeEntries(
    customsOffices :+ multipleOneValidOnePastOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ must contain(multipleOneValidOnePastOffice))
  }

  // Scenario 7: Multiple timetables — one valid/one invalid future → Yes
  it should "return office when one timetable is valid and another is future" in withCustomsOfficeEntries(
    customsOffices :+ multipleOneValidOneFutureOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ must contain(multipleOneValidOneFutureOffice))
  }

  // Scenario 8: Multiple timetables — one valid/one invalid no role → Yes
  it should "return office when one timetable matches role+date and another has wrong role" in withCustomsOfficeEntries(
    customsOffices :+ multipleOneValidOneNoRoleOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ must contain(multipleOneValidOneNoRoleOffice))
  }

  // Scenario 9: Multiple timetables — both invalid — past and future → No
  it should "not return office when both timetables have role but neither season covers roleDate" in withCustomsOfficeEntries(
    customsOffices :+ multipleBothInvalidPastFutureOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ mustNot contain(multipleBothInvalidPastFutureOffice))
  }

  // Scenario 10: Multiple timetables — both invalid — no roles → No
  it should "not return office when both timetables cover roleDate but neither has the role" in withCustomsOfficeEntries(
    customsOffices :+ multipleBothInvalidNoRolesOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = Some(testDate)
      )
      .map(_ mustNot contain(multipleBothInvalidNoRolesOffice))
  }

  // Verify roles-only filtering still works without roleDate (existing behaviour)
  it should "return offices filtered by roles only when no roleDate is specified (ignores season dates)" in withCustomsOfficeEntries(
    customsOffices :+ singleInvalidPastOffice
  ) { _ =>
    repository
      .fetchCustomsOfficeLists(
        referenceNumbers = None, countryCodes = None,
        roles = Some(Set("TRA")), activeAt = testActiveAt,
        phase = None,
        domain = None,
        roleDate = None
      )
      .map(_ must contain(singleInvalidPastOffice))
  }

  // V2 functionality
  "CustomsOfficesListsRepository.fetchCustomsOfficeSummaries" should "return only the summary fields of the Customs Office as a CustomsOfficeSumamry" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    val expectedSummary = CustomsOfficeSummary(
      customsOffices(0).referenceNumber,
      customsOffices(0).countryCode,
      customsOffices(0).customsOfficeLsd.customsOfficeUsualName,
      customsOffices(0).phase,
      customsOffices(0).domain
    )
    repository
      .fetchCustomsOfficeSummaries(defaultActiveAt, 1, customsOffices.length, None, None, None, None, None)
      .map { results =>
        {
          results(0) mustBe expectedSummary
        }
      }
  }

  "CustomsOfficesListsRepository.customsOfficesCount" should "return the count of active offices at the given instant" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository.customsOfficesCount(defaultActiveAt, None, None, None, None, None).map(_ mustBe 5L)
  }

  it should "return filtered count when referenceNumber is supplied" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository.customsOfficesCount(defaultActiveAt, referenceNumber = Some("DK003102"), None, None, None, None).map(_ mustBe 1L)
  }

  it should "return filtered count when countryCode is supplied" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository.customsOfficesCount(defaultActiveAt, countryCode = Some("DK"), None, None, None).map(_ mustBe 4L)
  }

  "CustomsOfficesListsRepository.fetchCustomsOfficeSummaries" should "filter by referenceNumber when supplied" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeSummaries(defaultActiveAt, 1, 10, referenceNumber = Some("DK003102"), None, None, None, None)
      .map { results =>
        results must have length 1
        results.head.referenceNumber mustBe "DK003102"
      }
  }

  it should "filter by countryCode when supplied" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeSummaries(defaultActiveAt, 1, 10, countryCode = Some("DK"), None, None, None)
      .map { results =>
        results must have length 4
        results.map(_.countryCode).distinct mustBe Seq("DK")
      }
  }

  it should "filter by officeName case-insensitively when supplied" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeSummaries(defaultActiveAt, 1, 10, officeName = Some("hirtshals"), None, None)
      .map { results =>
        results must have length 4
        results.map(_.referenceNumber) must contain allOf ("DK003102", "DK003103", "DK003104", "DK003105")
      }
  }

  it should "return no results when officeName matches nothing" in withCustomsOfficeEntries(
    customsOffices
  ) { _ =>
    repository
      .fetchCustomsOfficeSummaries(defaultActiveAt, 1, 10, officeName = Some("zzz-no-match"), None, None)
      .map(_ mustBe empty)
  }
}
