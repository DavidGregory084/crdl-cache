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
import uk.gov.hmrc.crdlcache.config.{AppConfig, ListConfig}
import uk.gov.hmrc.crdlcache.connectors.DpsConnector
import uk.gov.hmrc.crdlcache.models.*
import uk.gov.hmrc.crdlcache.models.CorrespondenceListInstruction.{
  InvalidateEntry,
  RecordMissingEntry,
  UpsertEntry
}
import uk.gov.hmrc.crdlcache.models.Operation.{Create, Delete, Invalidate, Update}
import uk.gov.hmrc.crdlcache.repositories.{CorrespondenceListsRepository, LastUpdatedRepository}
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.lock.MongoLockRepository

import java.time.{Clock, Instant, LocalDate, ZoneOffset}
import javax.inject.Inject
import scala.concurrent.ExecutionContext

class ImportCorrespondenceListsJob @Inject() (
  mongoComponent: MongoComponent,
  lockRepository: MongoLockRepository,
  lastUpdatedRepository: LastUpdatedRepository,
  correspondenceListsRepository: CorrespondenceListsRepository,
  dpsConnector: DpsConnector,
  appConfig: AppConfig,
  clock: Clock
)(using system: ActorSystem, ec: ExecutionContext)
  extends ImportCodeListsJob[CorrespondenceListKey, CorrespondenceListInstruction](
    "import-correspondence-lists",
    mongoComponent,
    lockRepository,
    lastUpdatedRepository,
    correspondenceListsRepository,
    dpsConnector,
    appConfig,
    clock
  ) {
  // The date of the full SEED extract used to populate the E200 list
  val SeedExtractDate: Instant = Instant.parse("2024-12-27T10:53:17Z")

  override protected def listConfigs: List[ListConfig] =
    appConfig.correspondenceListConfigs

  override protected def keyOfEntry(entry: CodeListSnapshotEntry): CorrespondenceListKey =
    CorrespondenceListKey(entry.key, entry.value, entry.phase, entry.domain)

  override protected def recordMissing(
    codeListCode: CodeListCode,
    mapping: CorrespondenceListKey
  ): CorrespondenceListInstruction = {
    // DPS provides no snapshot dates:
    // the best we can do with removed entries is to use the start of today as their deactivation date
    val CorrespondenceListKey(key, value, phase, domain) = mapping
    val startOfToday = LocalDate.now(clock).atStartOfDay(ZoneOffset.UTC)
    RecordMissingEntry(codeListCode, key, value, phase, domain, removedAt = startOfToday.toInstant)
  }

  protected def processEntry(
    codeListCode: CodeListCode,
    newEntry: CodeListSnapshotEntry
  ): CorrespondenceListInstruction = {
    val updatedEntry: CodeListEntry = CodeListEntry(
      codeListCode,
      newEntry.key,
      newEntry.value,
      newEntry.activeFrom,
      None,
      newEntry.updatedAt,
      newEntry.properties,
      newEntry.phase,
      newEntry.domain
    )

    newEntry.operation match {
      case Some(Create | Update)     => UpsertEntry(updatedEntry)
      case Some(Invalidate | Delete) => InvalidateEntry(updatedEntry)
      case None                      => UpsertEntry(updatedEntry)
    }
  }
}
