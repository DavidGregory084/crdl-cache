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

package uk.gov.hmrc.crdlcache.schedulers

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.ArgumentMatchers.{any, anyLong, eq as equalTo}
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.mongodb.scala.{ClientSession, MongoClient, MongoDatabase, SingleObservable}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar
import play.api.libs.json.Json
import uk.gov.hmrc.crdlcache.config.{AppConfig, CorrespondenceListConfig}
import uk.gov.hmrc.crdlcache.connectors.DpsConnector
import uk.gov.hmrc.crdlcache.models.*
import uk.gov.hmrc.crdlcache.models.CodeListCode.E200
import uk.gov.hmrc.crdlcache.models.CodeListOrigin.SEED
import uk.gov.hmrc.crdlcache.models.CorrespondenceListInstruction.{
  InvalidateEntry,
  RecordMissingEntry,
  UpsertEntry
}
import uk.gov.hmrc.crdlcache.models.Operation.Update
import uk.gov.hmrc.crdlcache.models.dps.codelist
import uk.gov.hmrc.crdlcache.models.dps.codelist.*
import uk.gov.hmrc.crdlcache.models.errors.MongoError
import uk.gov.hmrc.crdlcache.repositories.{CorrespondenceListsRepository, LastUpdatedRepository}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.lock.{Lock, MongoLockRepository}

import java.time.{Clock, Instant, LocalDate, ZoneOffset}
import scala.concurrent.{ExecutionContext, Future}

class ImportCorrespondenceListsJobSpec
  extends AnyFlatSpec
  with Matchers
  with MockitoSugar
  with ScalaFutures
  with IntegrationPatience
  with BeforeAndAfterEach {
  private val mongoComponent                = mock[MongoComponent]
  private val mongoClient                   = mock[MongoClient]
  private val mongoDatabase                 = mock[MongoDatabase]
  private val clientSession                 = mock[ClientSession]
  private val lockRepository                = mock[MongoLockRepository]
  private val lastUpdatedRepository         = mock[LastUpdatedRepository]
  private val correspondenceListsRepository = mock[CorrespondenceListsRepository]
  private val dpsConnector                  = mock[DpsConnector]
  private val appConfig                     = mock[AppConfig]
  private val fixedInstant                  = Instant.parse("2025-06-03T00:00:00Z")
  private val clock                         = Clock.fixed(fixedInstant, ZoneOffset.UTC)

  given ActorSystem      = ActorSystem("test")
  given ExecutionContext = ExecutionContext.global

  private val correspondenceListsJob = new ImportCorrespondenceListsJob(
    mongoComponent,
    lockRepository,
    lastUpdatedRepository,
    correspondenceListsRepository,
    dpsConnector,
    appConfig,
    clock
  )

  private val snapshotsPage1 = CodeListResponse(
    List(
      DpsCodeListSnapshot(
        E200,
        "CorrespondenceCnCodeExciseProduct",
        1,
        List(
          DpsCodeListEntry(
            List(
              codelist.DataItem("CnCode", Some("27101944")),
              codelist.DataItem("ExciseProductCode", Some("E430")),
              codelist.DataItem("Action_Operation", Some("C")),
              codelist.DataItem("Action_ActivationDate", Some("01-01-2025")),
              codelist.DataItem("Action_ActionIdentification", Some("433")),
              codelist.DataItem("Action_ResponsibleDataManager", None),
              codelist.DataItem("Action_ModificationDateAndTime", Some("30-12-2024"))
            ),
            List(LanguageDescription("en", "No english language description available"))
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("CnCode", Some("27101944")),
              codelist.DataItem("ExciseProductCode", Some("E440")),
              codelist.DataItem("Action_Operation", Some("C")),
              codelist.DataItem("Action_ActivationDate", Some("01-01-2025")),
              codelist.DataItem("Action_ActionIdentification", Some("437")),
              codelist.DataItem("Action_ResponsibleDataManager", None),
              codelist.DataItem("Action_ModificationDateAndTime", Some("30-12-2024"))
            ),
            List(codelist.LanguageDescription("en", "No english language description available"))
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("CnCode", Some("27102019")),
              codelist.DataItem("ExciseProductCode", Some("E430")),
              codelist.DataItem("Action_Operation", Some("U")),
              codelist.DataItem("Action_ActivationDate", Some("15-11-2013")),
              codelist.DataItem("Action_ActionIdentification", Some("432")),
              codelist.DataItem("Action_ResponsibleDataManager", None),
              codelist.DataItem("Action_ModificationDateAndTime", Some("14-11-2013"))
            ),
            List(LanguageDescription("en", "No english language description available"))
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("CnCode", Some("27102019")),
              codelist.DataItem("ExciseProductCode", Some("E440")),
              codelist.DataItem("Action_Operation", Some("U")),
              codelist.DataItem("Action_ActivationDate", Some("15-11-2013")),
              codelist.DataItem("Action_ActionIdentification", Some("437")),
              codelist.DataItem("Action_ResponsibleDataManager", None),
              codelist.DataItem("Action_ModificationDateAndTime", Some("14-11-2013"))
            ),
            List(codelist.LanguageDescription("en", "No english language description available"))
          )
        )
      )
    )
  )

  private val snapshotsPage2 = codelist.CodeListResponse(
    List(
      DpsCodeListSnapshot(
        E200,
        "CorrespondenceCnCodeExciseProduct",
        2,
        List(
          DpsCodeListEntry(
            List(
              codelist.DataItem("CnCode", Some("27101944")),
              codelist.DataItem("ExciseProductCode", Some("E430")),
              codelist.DataItem("Action_Operation", Some("I")),
              codelist.DataItem("Action_ActivationDate", Some("02-01-2025")),
              codelist.DataItem("Action_ActionIdentification", Some("2412")),
              codelist.DataItem("Action_ResponsibleDataManager", None),
              codelist.DataItem("Action_ModificationDateAndTime", Some("01-01-2025"))
            ),
            List(LanguageDescription("en", "No english language description available"))
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("CnCode", Some("27101944")),
              codelist.DataItem("ExciseProductCode", Some("E440")),
              codelist.DataItem("Action_Operation", Some("U")),
              codelist.DataItem("Action_ActivationDate", Some("01-01-2025")),
              codelist.DataItem("Action_ActionIdentification", Some("2413")),
              codelist.DataItem("Action_ResponsibleDataManager", None),
              codelist.DataItem("Action_ModificationDateAndTime", Some("30-12-2024"))
            ),
            List(codelist.LanguageDescription("en", "No english language description available"))
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("CnCode", Some("27102019")),
              codelist.DataItem("ExciseProductCode", Some("E440")),
              codelist.DataItem("Action_Operation", Some("U")),
              codelist.DataItem("Action_ActivationDate", Some("15-11-2013")),
              codelist.DataItem("Action_ActionIdentification", Some("437")),
              codelist.DataItem("Action_ResponsibleDataManager", None),
              codelist.DataItem("Action_ModificationDateAndTime", Some("14-11-2013"))
            ),
            List(LanguageDescription("en", "No english language description available"))
          )
        )
      )
    )
  )

  override def beforeEach(): Unit = {
    reset(
      mongoComponent,
      mongoClient,
      mongoDatabase,
      clientSession,
      lockRepository,
      lastUpdatedRepository,
      correspondenceListsRepository,
      dpsConnector,
      appConfig
    )

    when(correspondenceListsRepository.countEntries(any(), any(), any(), any()))
      .thenReturn(Future.successful(0))

    // Job lock
    val mockLock = mock[Lock]
    when(lockRepository.takeLock(any(), any(), any())).thenReturn(Future.successful(Some(mockLock)))
    when(lockRepository.releaseLock(any(), any())).thenReturn(Future.unit)

    // Transactions
    when(mongoComponent.client).thenReturn(mongoClient)
    when(mongoComponent.database).thenReturn(mongoDatabase)
    when(mongoClient.startSession(any())).thenReturn(SingleObservable(clientSession))
    when(clientSession.commitTransaction())
      .thenAnswer(_ => Source.empty[Void].runWith(Sink.asPublisher(fanout = false)))
    when(clientSession.abortTransaction())
      .thenAnswer(_ => Source.empty[Void].runWith(Sink.asPublisher(fanout = false)))
  }

  "ImportCodeListsJob.importCodeLists" should "import the configured correspondence lists when there is no last updated date stored" in {
    val lastUpdated        = LocalDate.of(2025, 3, 12)
    val lastUpdatedInstant = lastUpdated.atStartOfDay(ZoneOffset.UTC).toInstant

    // Last updated date
    when(appConfig.defaultLastUpdated).thenReturn(lastUpdated)

    when(lastUpdatedRepository.fetchLastUpdated(any(), any(), any()))
      .thenReturn(Future.successful(None))

    when(
      lastUpdatedRepository.setLastUpdated(
        equalTo(clientSession),
        any(),
        anyLong(),
        any(),
        any(),
        any()
      )
    )
      .thenReturn(Future.unit)

    // Correspondence list manipulation
    when(
      correspondenceListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(E200),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Set.empty[CorrespondenceListKey]))
      .thenReturn(
        Future.successful(
          Set(
            CorrespondenceListKey("27101944", "E430", None, None),
            CorrespondenceListKey("27101944", "E440", None, None),
            CorrespondenceListKey("27102019", "E430", None, None),
            CorrespondenceListKey("27102019", "E440", None, None),
          )
        )
      )

    when(correspondenceListsRepository.countEntries(any(), any(), any(), any()))
      .thenReturn(Future.successful(0L))

    // Correspondence list configuration
    when(appConfig.correspondenceListConfigs).thenReturn(
      List(
        CorrespondenceListConfig(E200, SEED, "CnCode", "ExciseProductCode")
      )
    )

    // DPS connector responses
    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(E200),
        equalTo(lastUpdatedInstant),
        equalTo(None),
        equalTo(None)
      )(using any())
    )
      .thenReturn(Source(List(snapshotsPage1, snapshotsPage2)))

    // Instruction execution
    when(
      correspondenceListsRepository.executeInstructions(
        equalTo(clientSession),
        any[List[CorrespondenceListInstruction]]
      )
    )
      .thenReturn(Future.unit)

    correspondenceListsJob.importCodeLists().futureValue

    verify(correspondenceListsRepository, times(2)).executeInstructions(
      equalTo(clientSession),
      any()
    )

    verify(correspondenceListsRepository, times(2))
      .executeInstructions(equalTo(clientSession), any())

    // There are two snapshots for E200
    verify(lastUpdatedRepository, times(2)).setLastUpdated(
      equalTo(clientSession),
      equalTo(E200),
      anyLong(),
      any(),
      any(),
      equalTo(fixedInstant)
    )

    // Two snapshots should have been committed
    verify(clientSession, times(2)).commitTransaction()
  }

  it should "import the configured correspondence lists when there is a last updated record stored" in {
    // Last updated date
    val storedInstant = Instant.parse("2025-03-13T00:00:00Z")

    when(appConfig.defaultLastUpdated).thenReturn(LocalDate.of(2025, 3, 12))

    when(lastUpdatedRepository.fetchLastUpdated(E200, None, None))
      .thenReturn(Future.successful(Some(LastUpdated(E200, 1, None, None, storedInstant))))

    when(
      lastUpdatedRepository.setLastUpdated(
        equalTo(clientSession),
        any(),
        anyLong(),
        any(),
        any(),
        any()
      )
    )
      .thenReturn(Future.unit)

    // Correspondence list manipulation
    when(
      correspondenceListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(E200),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Set.empty[CorrespondenceListKey]))
      .thenReturn(
        Future.successful(
          Set(
            CorrespondenceListKey("27101944", "E430", None, None),
            CorrespondenceListKey("27101944", "E440", None, None),
            CorrespondenceListKey("27102019", "E430", None, None),
            CorrespondenceListKey("27102019", "E440", None, None)
          )
        )
      )

    when(correspondenceListsRepository.countEntries(any(), any(), any(), any()))
      .thenReturn(Future.successful(0L))

    // Correspondence list configuration
    when(appConfig.correspondenceListConfigs).thenReturn(
      List(
        CorrespondenceListConfig(E200, SEED, "CnCode", "ExciseProductCode")
      )
    )

    // DPS connector responses
    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(E200),
        equalTo(storedInstant),
        equalTo(None),
        equalTo(None)
      )(using any())
    )
      .thenReturn(Source(List(snapshotsPage1, snapshotsPage2)))

    // Instruction execution
    when(
      correspondenceListsRepository.executeInstructions(
        equalTo(clientSession),
        any[List[CorrespondenceListInstruction]]
      )
    )
      .thenReturn(Future.unit)

    correspondenceListsJob.importCodeLists().futureValue

    // The static data is skipped because later data was imported already
    verify(lastUpdatedRepository, never()).setLastUpdated(
      equalTo(clientSession),
      equalTo(E200),
      anyLong(),
      equalTo(None),
      equalTo(None),
      equalTo(correspondenceListsJob.SeedExtractDate)
    )

    // There are two snapshots for E200, but we already have snapshot 1
    verify(correspondenceListsRepository, times(1))
      .executeInstructions(equalTo(clientSession), any())

    verify(lastUpdatedRepository, times(1)).setLastUpdated(
      equalTo(clientSession),
      equalTo(E200),
      equalTo(2L),
      equalTo(None),
      equalTo(None),
      equalTo(fixedInstant)
    )

    // Only one snapshot should have been committed
    verify(clientSession, times(1)).commitTransaction()
  }

  it should "roll back when there is an issue importing one of the correspondence list snapshots" in {
    val lastUpdated        = LocalDate.of(2025, 3, 12)
    val lastUpdatedInstant = lastUpdated.atStartOfDay(ZoneOffset.UTC).toInstant

    // Last updated date
    when(appConfig.defaultLastUpdated).thenReturn(lastUpdated)

    when(lastUpdatedRepository.fetchLastUpdated(any(), any(), any()))
      .thenReturn(Future.successful(None))

    when(
      lastUpdatedRepository.setLastUpdated(
        equalTo(clientSession),
        any(),
        anyLong(),
        any(),
        any(),
        any()
      )
    )
      .thenReturn(Future.unit)

    // Correspondence list manipulation
    when(
      correspondenceListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(E200),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Set.empty[CorrespondenceListKey]))
      .thenReturn(
        Future.successful(
          Set(
            CorrespondenceListKey("27101944", "E430", None, None),
            CorrespondenceListKey("27101944", "E440", None, None),
            CorrespondenceListKey("27102019", "E430", None, None),
            CorrespondenceListKey("27102019", "E440", None, None),
          )
        )
      )

    when(correspondenceListsRepository.countEntries(any(), any(), any(), any()))
      .thenReturn(Future.successful(0L))

    // Correspondence list configuration
    when(appConfig.correspondenceListConfigs).thenReturn(
      List(
        CorrespondenceListConfig(E200, SEED, "CnCode", "ExciseProductCode")
      )
    )

    // DPS connector responses
    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(E200),
        equalTo(lastUpdatedInstant),
        equalTo(None),
        equalTo(None)
      )(using any())
    )
      .thenReturn(Source(List(snapshotsPage1, snapshotsPage2)))

    // Instruction execution
    when(
      correspondenceListsRepository.executeInstructions(
        equalTo(clientSession),
        any[List[CorrespondenceListInstruction]]
      )
    )
      .thenReturn(Future.unit)
      .thenReturn(Future.failed(MongoError.NotAcknowledged))

    correspondenceListsJob.importCodeLists().futureValue

    verify(correspondenceListsRepository, times(2)).executeInstructions(
      equalTo(clientSession),
      any()
    )

    verify(correspondenceListsRepository, times(2))
      .executeInstructions(equalTo(clientSession), any())

    // There are two snapshots for E200, but the second snapshot import failed
    verify(lastUpdatedRepository, times(1)).setLastUpdated(
      equalTo(clientSession),
      equalTo(E200),
      equalTo(1L),
      equalTo(None),
      equalTo(None),
      equalTo(fixedInstant)
    )

    // The first DPS snapshot should have been committed, but the second should be rolled back.
    val inOrder = Mockito.inOrder(clientSession)
    inOrder.verify(clientSession, times(1)).commitTransaction()
    inOrder.verify(clientSession, times(1)).abortTransaction()
  }

  "ImportCodeListsJob.processSnapshot" should "produce a list of instructions for a snapshot that contains only new entries" in {
    when(
      correspondenceListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(E200),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(Future.successful(Set.empty[CorrespondenceListKey]))

    val correspondenceListConfig =
      CorrespondenceListConfig(E200, SEED, "CnCode", "ExciseProductCode")

    val instructions = correspondenceListsJob
      .processSnapshot(
        clientSession,
        correspondenceListConfig,
        CodeListSnapshot.fromDpsSnapshot(correspondenceListConfig, snapshotsPage1.elements.head)
      )
      .futureValue

    instructions.sortBy(ins => (ins.key, ins.value, ins.activeFrom)) mustBe List(
      UpsertEntry(
        CodeListEntry(
          E200,
          "27101944",
          "E430",
          Instant.parse("2025-01-01T00:00:00Z"),
          None,
          Some(Instant.parse("2024-12-30T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "433"
          ),
          None,
          None
        )
      ),
      UpsertEntry(
        CodeListEntry(
          E200,
          "27101944",
          "E440",
          Instant.parse("2025-01-01T00:00:00Z"),
          None,
          Some(Instant.parse("2024-12-30T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "437"
          ),
          None,
          None
        )
      ),
      UpsertEntry(
        CodeListEntry(
          E200,
          "27102019",
          "E430",
          Instant.parse("2013-11-15T00:00:00Z"),
          None,
          Some(Instant.parse("2013-11-14T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "432"
          ),
          None,
          None
        )
      ),
      UpsertEntry(
        CodeListEntry(
          E200,
          "27102019",
          "E440",
          Instant.parse("2013-11-15T00:00:00Z"),
          None,
          Some(Instant.parse("2013-11-14T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "437"
          ),
          None,
          None
        )
      )
    )
  }

  it should "produce a list of instructions for a snapshot which contains invalidations and missing entries" in {
    when(
      correspondenceListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(E200),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(
        Future.successful(
          Set(
            CorrespondenceListKey("27101944", "E430", None, None),
            CorrespondenceListKey("27101944", "E440", None, None),
            CorrespondenceListKey("27102019", "E430", None, None),
            CorrespondenceListKey("27102019", "E440", None, None),
          )
        )
      )

    val correspondenceListConfig =
      CorrespondenceListConfig(E200, SEED, "CnCode", "ExciseProductCode")

    val instructions = correspondenceListsJob
      .processSnapshot(
        clientSession,
        correspondenceListConfig,
        CodeListSnapshot.fromDpsSnapshot(correspondenceListConfig, snapshotsPage2.elements.head)
      )
      .futureValue

    instructions.sortBy(ins => (ins.key, ins.value, ins.activeFrom)) mustBe List(
      InvalidateEntry(
        CodeListEntry(
          E200,
          "27101944",
          "E430",
          Instant.parse("2025-01-02T00:00:00Z"),
          None,
          Some(Instant.parse("2025-01-01T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "2412"
          ),
          None,
          None
        )
      ),
      UpsertEntry(
        CodeListEntry(
          E200,
          "27101944",
          "E440",
          Instant.parse("2025-01-01T00:00:00Z"),
          None,
          Some(Instant.parse("2024-12-30T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "2413"
          ),
          None,
          None
        )
      ),
      RecordMissingEntry(E200, "27102019", "E430", None, None, fixedInstant),
      UpsertEntry(
        CodeListEntry(
          E200,
          "27102019",
          "E440",
          Instant.parse("2013-11-15T00:00:00Z"),
          None,
          Some(Instant.parse("2013-11-14T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "437"
          ),
          None,
          None
        )
      )
    )
  }

  it should "pick the latest entry by modification date and action identification when there are duplicate entries for a given key->value mapping and activation date" in {
    when(
      correspondenceListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(E200),
        equalTo(None),
        equalTo(None)
      )
    )
      .thenReturn(
        Future.successful(
          Set(CorrespondenceListKey("27101966", "E470", None, None))
        )
      )

    val correspondenceListConfig =
      CorrespondenceListConfig(E200, SEED, "CnCode", "ExciseProductCode")

    val instructions = correspondenceListsJob
      .processSnapshot(
        clientSession,
        correspondenceListConfig,
        CodeListSnapshot(
          E200,
          "CorrespondenceCnCodeExciseProduct",
          1,
          Set(
            CodeListSnapshotEntry(
              "27101966",
              "E470",
              Instant.parse("2023-11-02T00:00:00Z"),
              Some(Instant.parse("2023-10-31T00:00:00Z")),
              Some(Update),
              Json.obj(
                "actionIdentification" -> "1093"
              ),
              None,
              None
            ),
            CodeListSnapshotEntry(
              "27101966",
              "E470",
              Instant.parse("2023-11-02T00:00:00Z"),
              Some(Instant.parse("2023-11-01T00:00:00Z")),
              Some(Update),
              Json.obj(
                "actionIdentification" -> "1096"
              ),
              None,
              None
            ),
            CodeListSnapshotEntry(
              "27101966",
              "E470",
              Instant.parse("2023-11-02T00:00:00Z"),
              Some(Instant.parse("2023-11-01T00:00:00Z")),
              Some(Update),
              Json.obj(
                "actionIdentification" -> "1099"
              ),
              None,
              None
            )
          )
        )
      )
      .futureValue

    instructions mustBe List(
      UpsertEntry(
        CodeListEntry(
          E200,
          "27101966",
          "E470",
          Instant.parse("2023-11-02T00:00:00Z"),
          None,
          Some(Instant.parse("2023-11-01T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "1099"
          ),
          None,
          None
        )
      )
    )
  }
}
