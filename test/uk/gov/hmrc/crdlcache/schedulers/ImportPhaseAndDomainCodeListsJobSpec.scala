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
import uk.gov.hmrc.crdlcache.config.{AppConfig, PhaseAndDomainListConfig}
import uk.gov.hmrc.crdlcache.connectors.DpsConnector
import uk.gov.hmrc.crdlcache.models.*
import uk.gov.hmrc.crdlcache.models.CodeListCode.{CL231, CL234}
import uk.gov.hmrc.crdlcache.models.CodeListOrigin.CSRD2
import uk.gov.hmrc.crdlcache.models.Instruction.{RecordMissingEntry, UpsertEntry}
import uk.gov.hmrc.crdlcache.models.Operation.Update
import uk.gov.hmrc.crdlcache.models.dps.codelist
import uk.gov.hmrc.crdlcache.models.dps.codelist.*
import uk.gov.hmrc.crdlcache.models.errors.MongoError
import uk.gov.hmrc.crdlcache.repositories.{LastUpdatedRepository, StandardCodeListsRepository}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.lock.{Lock, MongoLockRepository}

import java.time.{Clock, Instant, LocalDate, ZoneOffset}
import scala.concurrent.{ExecutionContext, Future}

class ImportPhaseAndDomainCodeListsJobSpec
  extends AnyFlatSpec
  with Matchers
  with MockitoSugar
  with ScalaFutures
  with IntegrationPatience
  with BeforeAndAfterEach {
  private val mongoComponent        = mock[MongoComponent]
  private val mongoClient           = mock[MongoClient]
  private val mongoDatabase         = mock[MongoDatabase]
  private val clientSession         = mock[ClientSession]
  private val lockRepository        = mock[MongoLockRepository]
  private val lastUpdatedRepository = mock[LastUpdatedRepository]
  private val codeListsRepository   = mock[StandardCodeListsRepository]
  private val dpsConnector          = mock[DpsConnector]
  private val appConfig             = mock[AppConfig]
  private val fixedInstant          = Instant.parse("2025-06-03T00:00:00Z")
  private val clock                 = Clock.fixed(fixedInstant, ZoneOffset.UTC)

  given ActorSystem      = ActorSystem("test")
  given ExecutionContext = ExecutionContext.global

  private val codeListsJob = new ImportPhaseAndDomainCodeListsJob(
    mongoComponent,
    lockRepository,
    lastUpdatedRepository,
    codeListsRepository,
    dpsConnector,
    appConfig,
    clock
  )

  private val snapshotsPage1 = CodeListResponse(
    List(
      DpsCodeListSnapshot(
        CL231,
        "DeclarationType",
        1,
        List(
          DpsCodeListEntry(
            List(
              DataItem("DeclarationTypeCode", Some("T")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(
              LanguageDescription(
                "en",
                "Mixed consignments comprising both goods to be placed under external Union transit procedure and goods which are to be placed under the internal Union transit procedure."
              )
            )
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T1")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(
              codelist.LanguageDescription(
                "en",
                "Goods not having the customs status of Union goods, which are placed under the common transit procedure."
              )
            )
          )
        )
      )
    )
  )

  private val snapshotsPage2 = codelist.CodeListResponse(
    List(
      DpsCodeListSnapshot(
        CL231,
        "DeclarationType",
        2,
        List(
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T2")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(
              codelist.LanguageDescription(
                "en",
                "Goods having the customs status of Union goods, which are placed under the common transit procedure"
              )
            )
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T2F")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(
              codelist.LanguageDescription(
                "en",
                "Goods required to move under the internal Union transit procedure, in accordance with Article 188 of Delegated Regulation (EU) 2015/2446"
              )
            )
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T2SM")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(
              codelist.LanguageDescription(
                "en",
                "Goods placed under the internal Union transit procedure, in application of Article 2 of Decision 4/92/EC of the EEC-San Marino Cooperation Committee of 22 December 1992"
              )
            )
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("TIR")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(codelist.LanguageDescription("en", "TIR carnet"))
          )
        )
      )
    )
  )

  private val snapshotsPage2WithInvalidations = codelist.CodeListResponse(
    List(
      DpsCodeListSnapshot(
        CL231,
        "DeclarationType",
        2,
        List(
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T2")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(
              codelist.LanguageDescription("en", "Goods having the customs status of Union goods")
            )
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T2F")),
              codelist.DataItem("RDEntryStatus_state", Some("invalid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(
              codelist.LanguageDescription("en", "Goods required to move under internal transit")
            )
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T2SM")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(codelist.LanguageDescription("en", "Goods placed under internal Union transit"))
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("T2SM")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(codelist.LanguageDescription("en", "Goods placed under internal Union transit"))
          ),
          DpsCodeListEntry(
            List(
              codelist.DataItem("DeclarationTypeCode", Some("TIR")),
              codelist.DataItem("RDEntryStatus_state", Some("valid")),
              codelist.DataItem("RDEntryStatus_activeFrom", Some("01-11-2025")),
              codelist.DataItem("Phase", Some("P6")),
              codelist.DataItem("Domain", Some("NCTS"))
            ),
            List(codelist.LanguageDescription("en", "TIR carnet"))
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
      codeListsRepository,
      dpsConnector,
      appConfig
    )

    when(codeListsRepository.countEntries(any(), any(), any(), any()))
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

  "ImportCodeListsJob.importCodeLists" should "import the configured codelists when there is no last updated date stored" in {
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
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS)),
        equalTo(fixedInstant)
      )
    )
      .thenReturn(Future.unit)

    // Codelist manipulation
    when(
      codeListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(CL231),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(Set.empty[String]))
      .thenReturn(
        Future.successful(
          Set(
            CodeListKey("T2", Some(Phase.NCTS6), Some(Domain.NCTS)),
            CodeListKey("T2F", Some(Phase.NCTS6), Some(Domain.NCTS))
          )
        )
      )

    when(codeListsRepository.countEntries(any(), any(), any(), any()))
      .thenReturn(Future.successful(0L))

    // Codelist configuration
    when(appConfig.phaseAndDomainListConfigs).thenReturn(
      List(
        PhaseAndDomainListConfig(CL231, CSRD2, "DeclarationTypeCode", Phase.NCTS6, Domain.NCTS),
        PhaseAndDomainListConfig(CL234, CSRD2, "PreviousDocumentTypeCode", Phase.NCTS6, Domain.NCTS)
      )
    )

    // DPS connector responses
    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(CL231),
        equalTo(lastUpdatedInstant),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )(using any())
    )
      .thenReturn(Source(List(snapshotsPage1, snapshotsPage2)))

    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(CL234),
        equalTo(lastUpdatedInstant),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )(using any())
    )
      .thenReturn(Source.empty)

    // Instruction execution
    when(codeListsRepository.executeInstructions(equalTo(clientSession), any[List[Instruction]]))
      .thenReturn(Future.unit)

    codeListsJob.importCodeLists().futureValue

    verify(codeListsRepository, times(1)).executeInstructions(
      equalTo(clientSession),
      equalTo(
        List(
          UpsertEntry(
            CodeListEntry(
              CL231,
              "T",
              "Mixed consignments comprising both goods to be placed under external Union transit procedure and goods which are to be placed under the internal Union transit procedure.",
              Instant.parse("2025-11-01T00:00:00Z"),
              None,
              None,
              Json.obj(
                "state"  -> "valid",
                "phase"  -> "P6",
                "domain" -> "NCTS"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            )
          ),
          UpsertEntry(
            CodeListEntry(
              CL231,
              "T1",
              "Goods not having the customs status of Union goods, which are placed under the common transit procedure.",
              Instant.parse("2025-11-01T00:00:00Z"),
              None,
              None,
              Json.obj(
                "state"  -> "valid",
                "phase"  -> "P6",
                "domain" -> "NCTS"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            )
          )
        )
      )
    )

    verify(codeListsRepository, times(2)).executeInstructions(equalTo(clientSession), any())

    // There are two snapshots for CL231
    verify(lastUpdatedRepository, times(2)).setLastUpdated(
      equalTo(clientSession),
      equalTo(CL231),
      anyLong(),
      equalTo(Some(Phase.NCTS6)),
      equalTo(Some(Domain.NCTS)),
      equalTo(fixedInstant)
    )

    // There are no snapshots for CL234
    verify(lastUpdatedRepository, never()).setLastUpdated(
      equalTo(clientSession),
      equalTo(CL234),
      anyLong(),
      equalTo(Some(Phase.NCTS6)),
      equalTo(Some(Domain.NCTS)),
      equalTo(fixedInstant)
    )

    // Two snapshots should have been committed
    verify(clientSession, times(2)).commitTransaction()
  }

  it should "import the configured codelists when there is a last updated record stored" in {
    // Last updated date
    val storedInstant = Instant.parse("2025-03-13T00:00:00Z")

    when(appConfig.defaultLastUpdated).thenReturn(LocalDate.of(2025, 3, 12))

    when(lastUpdatedRepository.fetchLastUpdated(CL231, Some(Phase.NCTS6), Some(Domain.NCTS)))
      .thenReturn(
        Future.successful(Some(LastUpdated(CL231, 1, Some(Phase.NCTS6), Some(Domain.NCTS), storedInstant)))
      )

    when(lastUpdatedRepository.fetchLastUpdated(CL234, Some(Phase.NCTS6), Some(Domain.NCTS)))
      .thenReturn(
        Future.successful(Some(LastUpdated(CL234, 1, Some(Phase.NCTS6), Some(Domain.NCTS), storedInstant)))
      )

    when(
      lastUpdatedRepository.setLastUpdated(
        equalTo(clientSession),
        any(),
        anyLong(),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS)),
        equalTo(fixedInstant)
      )
    )
      .thenReturn(Future.unit)

    // Codelist manipulation
    when(
      codeListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(CL231),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(Set.empty[String]))
      .thenReturn(
        Future.successful(
          Set(
            CodeListKey("T2", Some(Phase.NCTS6), Some(Domain.NCTS)),
            CodeListKey("T2F", Some(Phase.NCTS6), Some(Domain.NCTS))
          )
        )
      )

    // Codelist configuration
    when(appConfig.phaseAndDomainListConfigs).thenReturn(
      List(
        PhaseAndDomainListConfig(CL231, CSRD2, "DeclarationType", Phase.NCTS6, Domain.NCTS),
        PhaseAndDomainListConfig(CL234, CSRD2, "DocumentTypeExcise", Phase.NCTS6, Domain.NCTS)
      )
    )

    // DPS connector responses
    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(CL231),
        equalTo(storedInstant),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )(using any())
    )
      .thenReturn(Source(List(snapshotsPage1, snapshotsPage2)))

    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(CL234),
        equalTo(storedInstant),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )(using any())
    )
      .thenReturn(Source.empty)

    // Instruction execution
    when(codeListsRepository.executeInstructions(equalTo(clientSession), any[List[Instruction]]))
      .thenReturn(Future.unit)

    codeListsJob.importCodeLists().futureValue
  }

  it should "roll back when there is an issue importing one of the codelist snapshots" in {
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
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS)),
        equalTo(fixedInstant)
      )
    )
      .thenReturn(Future.unit)

    // Codelist manipulation
    when(
      codeListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(CL231),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(Set.empty[String]))
      .thenReturn(
        Future.successful(
          Set(
            CodeListKey("T2", Some(Phase.NCTS6), Some(Domain.NCTS)),
            CodeListKey("T2F", Some(Phase.NCTS6), Some(Domain.NCTS))
          )
        )
      )

    when(codeListsRepository.countEntries(any(), any(), any(), any()))
      .thenReturn(Future.successful(0L))

    // Codelist configuration
    when(appConfig.phaseAndDomainListConfigs).thenReturn(
      List(
        PhaseAndDomainListConfig(CL231, CSRD2, "DeclarationTypeCode", Phase.NCTS6, Domain.NCTS),
        PhaseAndDomainListConfig(CL234, CSRD2, "PreviousDocumentTypeCode", Phase.NCTS6, Domain.NCTS)
      )
    )

    // DPS connector responses
    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(CL231),
        equalTo(lastUpdatedInstant),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )(using any())
    )
      .thenReturn(Source(List(snapshotsPage1, snapshotsPage2)))

    when(
      dpsConnector.fetchCodeListSnapshots(
        equalTo(CL234),
        equalTo(lastUpdatedInstant),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )(using any())
    )
      .thenReturn(Source.empty)

    // Instruction execution
    when(codeListsRepository.executeInstructions(equalTo(clientSession), any[List[Instruction]]))
      .thenReturn(Future.unit)
      .thenReturn(Future.failed(MongoError.NotAcknowledged))

    codeListsJob.importCodeLists().futureValue

    verify(codeListsRepository, times(1)).executeInstructions(
      equalTo(clientSession),
      equalTo(
        List(
          UpsertEntry(
            CodeListEntry(
              CL231,
              "T2",
              "Goods having the customs status of Union goods, which are placed under the common transit procedure",
              Instant.parse("2025-11-01T00:00:00Z"),
              None,
              None,
              Json.obj(
                "state"  -> "valid",
                "phase"  -> "P6",
                "domain" -> "NCTS"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            )
          ),
          UpsertEntry(
            CodeListEntry(
              CL231,
              "T2F",
              "Goods required to move under the internal Union transit procedure, in accordance with Article 188 of Delegated Regulation (EU) 2015/2446",
              Instant.parse("2025-11-01T00:00:00Z"),
              None,
              None,
              Json.obj(
                "state"  -> "valid",
                "phase"  -> "P6",
                "domain" -> "NCTS"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            )
          ),
          UpsertEntry(
            CodeListEntry(
              CL231,
              "T2SM",
              "Goods placed under the internal Union transit procedure, in application of Article 2 of Decision 4/92/EC of the EEC-San Marino Cooperation Committee of 22 December 1992",
              Instant.parse("2025-11-01T00:00:00Z"),
              None,
              None,
              Json.obj(
                "state"  -> "valid",
                "phase"  -> "P6",
                "domain" -> "NCTS"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            )
          ),
          UpsertEntry(
            CodeListEntry(
              CL231,
              "TIR",
              "TIR carnet",
              Instant.parse("2025-11-01T00:00:00Z"),
              None,
              None,
              Json.obj(
                "state"  -> "valid",
                "phase"  -> "P6",
                "domain" -> "NCTS"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            )
          )
        )
      )
    )

    verify(codeListsRepository, times(2)).executeInstructions(equalTo(clientSession), any())

    // There are two snapshots for CL231, but the second snapshot import failed
    verify(lastUpdatedRepository, times(1)).setLastUpdated(
      equalTo(clientSession),
      equalTo(CL231),
      equalTo(1L),
      equalTo(Some(Phase.NCTS6)),
      equalTo(Some(Domain.NCTS)),
      equalTo(fixedInstant)
    )

    // There are no snapshots for CL234
    verify(lastUpdatedRepository, never()).setLastUpdated(
      equalTo(clientSession),
      equalTo(CL234),
      anyLong(),
      equalTo(Some(Phase.NCTS6)),
      equalTo(Some(Domain.NCTS)),
      equalTo(fixedInstant)
    )

    // The first snapshot should have been committed, but the second should be rolled back
    val inOrder = Mockito.inOrder(clientSession)
    inOrder.verify(clientSession, times(1)).commitTransaction()
    inOrder.verify(clientSession, times(1)).abortTransaction()
  }

  "ImportPhaseAndDomainCodeListsJob.processSnapshot" should "produce a list of instructions for a snapshot that contains only new entries" in {
    when(
      codeListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(CL231),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      // .thenReturn(Future.successful(Set(CodeListKey("T", Some(Phase.NCTS6), Some(Domain.NCTS)))))
      .thenReturn(Future.successful(Set.empty[CodeListKey]))

    val codeListConfig = PhaseAndDomainListConfig(CL231, CSRD2, "DeclarationTypeCode", Phase.NCTS6, Domain.NCTS)

    val instructions = codeListsJob
      .processSnapshot(
        clientSession,
        codeListConfig,
        CodeListSnapshot.fromDpsSnapshot(codeListConfig, snapshotsPage1.elements.head),
        Some(codeListConfig.phase),
        Some(codeListConfig.domain)
      )
      .futureValue

    instructions.sortBy(ins => (ins.key, ins.activeFrom)) mustBe List(
      UpsertEntry(
        CodeListEntry(
          CL231,
          "T",
          "Mixed consignments comprising both goods to be placed under external Union transit procedure and goods which are to be placed under the internal Union transit procedure.",
          Instant.parse("2025-11-01T00:00:00Z"),
          None,
          None,
          Json.obj(
            "state"  -> "valid",
            "phase"  -> "P6",
            "domain" -> "NCTS"
          ),
          Some(Phase.NCTS6),
          Some(Domain.NCTS)
        )
      ),
      UpsertEntry(
        CodeListEntry(
          CL231,
          "T1",
          "Goods not having the customs status of Union goods, which are placed under the common transit procedure.",
          Instant.parse("2025-11-01T00:00:00Z"),
          None,
          None,
          Json.obj(
            "state"  -> "valid",
            "phase"  -> "P6",
            "domain" -> "NCTS"
          ),
          Some(Phase.NCTS6),
          Some(Domain.NCTS)
        )
      )
    )
  }

  it should "produce a list of instructions for a snapshot which contains invalidations and missing entries" in {
    when(
      codeListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(CL231),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(
        Future.successful(
          Set(
            CodeListKey("T", Some(Phase.NCTS6), Some(Domain.NCTS)),
            CodeListKey("T2", Some(Phase.NCTS6), Some(Domain.NCTS)),
            CodeListKey("T2F", Some(Phase.NCTS6), Some(Domain.NCTS))
          )
        )
      )

    val codeListConfig = PhaseAndDomainListConfig(CL231, CSRD2, "DeclarationTypeCode", Phase.NCTS6, Domain.NCTS)

    val instructions = codeListsJob
      .processSnapshot(
        clientSession,
        codeListConfig,
        CodeListSnapshot
          .fromDpsSnapshot(codeListConfig, snapshotsPage2WithInvalidations.elements.head),
        Some(codeListConfig.phase),
        Some(codeListConfig.domain)
      )
      .futureValue

    instructions.sortBy(ins => (ins.key, ins.activeFrom)) mustBe List(
      RecordMissingEntry(
        CL231,
        "T",
        Some(Phase.NCTS6),
        Some(Domain.NCTS),
        Instant.parse("2025-06-03T00:00:00Z")
      ),
      UpsertEntry(
        CodeListEntry(
          CL231,
          "T2",
          "Goods having the customs status of Union goods",
          Instant.parse("2025-11-01T00:00:00Z"),
          None,
          None,
          Json.obj(
            "state"  -> "valid",
            "phase"  -> "P6",
            "domain" -> "NCTS"
          ),
          Some(Phase.NCTS6),
          Some(Domain.NCTS)
        )
      ),
      UpsertEntry(
        CodeListEntry(
          CL231,
          "T2F",
          "Goods required to move under internal transit",
          Instant.parse("2025-11-01T00:00:00Z"),
          None,
          None,
          Json.obj(
            "state"  -> "invalid",
            "phase"  -> "P6",
            "domain" -> "NCTS"
          ),
          Some(Phase.NCTS6),
          Some(Domain.NCTS)
        )
      ),
      UpsertEntry(
        CodeListEntry(
          CL231,
          "T2SM",
          "Goods placed under internal Union transit",
          Instant.parse("2025-11-01T00:00:00Z"),
          None,
          None,
          Json.obj(
            "state"  -> "valid",
            "phase"  -> "P6",
            "domain" -> "NCTS"
          ),
          Some(Phase.NCTS6),
          Some(Domain.NCTS)
        )
      ),
      UpsertEntry(
        CodeListEntry(
          CL231,
          "TIR",
          "TIR carnet",
          Instant.parse("2025-11-01T00:00:00Z"),
          None,
          None,
          Json.obj(
            "state"  -> "valid",
            "phase"  -> "P6",
            "domain" -> "NCTS"
          ),
          Some(Phase.NCTS6),
          Some(Domain.NCTS)
        )
      )
    )
  }

  it should "pick the latest entry by modification date and action identification when there are duplicate entries for a given key and activation date" in {
    when(
      codeListsRepository.fetchEntryKeys(
        equalTo(clientSession),
        equalTo(CL234),
        equalTo(Some(Phase.NCTS6)),
        equalTo(Some(Domain.NCTS))
      )
    )
      .thenReturn(Future.successful(Set(CodeListKey("E470", Some(Phase.NCTS6), Some(Domain.NCTS)))))

    val codeListConfig = PhaseAndDomainListConfig(CL234, CSRD2, "DocumentTypeExcise", Phase.NCTS6, Domain.NCTS)

    val instructions = codeListsJob
      .processSnapshot(
        clientSession,
        codeListConfig,
        CodeListSnapshot(
          CL234,
          "ExciseProducts",
          1,
          Set(
            CodeListSnapshotEntry(
              "E470",
              "Heavy fuel oil falling within CN codes 2710 19 62, 2710 19 66, 2710 19 67, 2710 20 32 and 2710 20 38 (Article 20(1)(c) of Directive 2003/96/EC)",
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
              "E470",
              "Heavy fuel oil falling within CN codes 2710 19 62, 2710 19 66, 2710 19 67, 2710 20 32 and 2710 20 38 (Article 20(1)(c) of Directive 2003/96/EC)",
              Instant.parse("2023-11-02T00:00:00Z"),
              Some(Instant.parse("2023-11-01T00:00:00Z")),
              Some(Update),
              Json.obj(
                "actionIdentification" -> "1096"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            ),
            CodeListSnapshotEntry(
              "E470",
              "Heavy fuel oil falling within CN codes 2710 19 62, 2710 19 66, 2710 19 67, 2710 20 32 and 2710 20 38 (Article 20(1)(c) of Directive 2003/96/EC)",
              Instant.parse("2023-11-02T00:00:00Z"),
              Some(Instant.parse("2023-11-01T00:00:00Z")),
              Some(Update),
              Json.obj(
                "actionIdentification" -> "1099"
              ),
              Some(Phase.NCTS6),
              Some(Domain.NCTS)
            )
          )
        ),
        Some(Phase.NCTS6),
        Some(Domain.NCTS)
      )
      .futureValue

    instructions mustBe List(
      UpsertEntry(
        CodeListEntry(
          CL234,
          "E470",
          "Heavy fuel oil falling within CN codes 2710 19 62, 2710 19 66, 2710 19 67, 2710 20 32 and 2710 20 38 (Article 20(1)(c) of Directive 2003/96/EC)",
          Instant.parse("2023-11-02T00:00:00Z"),
          None,
          Some(Instant.parse("2023-11-01T00:00:00Z")),
          Json.obj(
            "actionIdentification" -> "1099"
          ),
          Some(Phase.NCTS6),
          Some(Domain.NCTS)
        )
      )
    )
  }
}
