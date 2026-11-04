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
import org.mongodb.scala.model.Filters.and
import org.mongodb.scala.model.{Filters, Sorts}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Assertion, OptionValues}
import play.api.libs.json.{JsString, Json}
import uk.gov.hmrc.crdlcache.models.CodeListCode.{E200, code}
import uk.gov.hmrc.crdlcache.models.CodeListEntry
import uk.gov.hmrc.crdlcache.models.CorrespondenceListInstruction.{
  InvalidateEntry,
  RecordMissingEntry,
  UpsertEntry
}
import uk.gov.hmrc.mongo.test.{
  CleanMongoCollectionSupport,
  IndexedMongoQueriesSupport,
  PlayMongoRepositorySupport
}
import uk.gov.hmrc.mongo.transaction.TransactionConfiguration

import java.time.Instant
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class CorrespondenceListsRepositorySpec
  extends AnyFlatSpec
  with PlayMongoRepositorySupport[CodeListEntry]
  with CleanMongoCollectionSupport
  with IndexedMongoQueriesSupport
  with Matchers
  with OptionValues
  with ScalaFutures {

  given TransactionConfiguration = TransactionConfiguration.strict
  given ec: ExecutionContext     = ExecutionContext.global

  override protected val repository: CorrespondenceListsRepository =
    new CorrespondenceListsRepository(
      mongoComponent
    )

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 30.seconds, interval = 100.millis)

  def withCorrespondenceListEntries(
    entries: Seq[CodeListEntry]
  )(test: ClientSession => Future[Assertion]): Unit = {
    repository.collection.insertMany(entries).toFuture.futureValue
    repository.withSessionAndTransaction(test).futureValue
  }

  private val activeEntries = List(
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
    ),
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
    ),
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
    ),
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

  private val supersededListEntries = List(
    CodeListEntry(
      E200,
      "27101944",
      "E420",
      Instant.parse("2024-01-15T00:00:00Z"),
      Some(Instant.parse("2024-01-17T00:00:00Z")),
      Some(Instant.parse("2024-01-14T00:00:00Z")),
      Json.obj(
        "actionIdentification" -> "1234"
      ),
      None,
      None
    )
  )

  private val activeEntry = CodeListEntry(
    E200,
    "22060039",
    "W200",
    Instant.parse("2024-01-31T00:00:00Z"),
    None,
    Some(Instant.parse("2024-01-17T00:00:00Z")),
    Json.obj(
      "actionIdentification" -> "1024"
    ),
    None,
    None
  )

  private val invalidatedEntry = CodeListEntry(
    E200,
    "27101944",
    "E460",
    Instant.parse("2024-01-31T00:00:00Z"),
    Some(Instant.parse("2026-05-22T00:00:00Z")),
    Some(Instant.parse("2026-05-21T00:00:00Z")),
    Json.obj(
      "actionIdentification" -> "9999"
    ),
    None,
    None
  )

  private val existingE470Entry = CodeListEntry(
    E200,
    "27101944",
    "E470",
    Instant.parse("2025-06-05T00:00:00Z"),
    None,
    None,
    Json.obj(
      "actionIdentification" -> "7777"
    ),
    None,
    None
  )

  private val postDatedE470Entry = CodeListEntry(
    E200,
    "27101944",
    "E470",
    Instant.parse("2026-06-05T00:00:00Z"),
    None,
    None,
    Json.obj(
      "actionIdentification" -> "9999"
    ),
    None,
    None
  )

  private val newCorrespondenceEntry = CodeListEntry(
    E200,
    "22042196",
    "I000",
    Instant.parse("2024-01-31T00:00:00Z"),
    None,
    Some(Instant.parse("2024-01-17T00:00:00Z")),
    Json.obj(
      "actionIdentification" -> "1024"
    ),
    None,
    None
  )

  private val entriesWithNoEndDate = activeEntries :+ postDatedE470Entry

  private val correspondenceListEntries =
    activeEntries ++ supersededListEntries :+ invalidatedEntry :+ postDatedE470Entry

  "CorrespondenceListsRepository.fetchEntryKeys" should "return entries that have not been superseded" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { session =>
    repository.fetchEntryKeys(session, E200).map(_ must contain("27101944" -> "E440"))
  }

  it should "not return entries that are invalidated" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { session =>
    repository.fetchEntryKeys(session, E200).map(_ mustNot contain("27101944" -> "E460"))
  }

  it should "contain the active code list entry keys" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { session =>
    repository
      .fetchEntryKeys(session, E200)
      .map {
        _ mustBe entriesWithNoEndDate
          .map(entry => (entry.key, entry.value))
          .toSet
      }
  }

  "CorrespondenceListsRepository.fetchEntries" should "return the codelist entries whose activeFrom date is before the requested date" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = None,
        filterProperties = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ must contain allElementsOf activeEntries)
  }

  it should "apply filtering of entries according to the supplied keys" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = Some(Set("27101944")),
        filterProperties = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ must contain allElementsOf activeEntries.take(2))
  }

  it should "not apply filtering of entries when the set of supplied keys is empty" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = Some(Set.empty),
        filterProperties = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ must contain allElementsOf activeEntries)
  }

  it should "not return entries that have been superseded" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = None,
        filterProperties = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ must contain noElementsOf supersededListEntries)
  }

  it should "not return entries that are not yet active" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = None,
        filterProperties = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ mustNot contain(postDatedE470Entry))
  }

  it should "return entries that have been invalidated if the invalidation date is in the future" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = None,
        filterProperties = None,
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ must contain(invalidatedEntry))
  }

  it should "apply filtering of entries using properties" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = None,
        filterProperties = Some(Map("actionIdentification" -> JsString("437"))),
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ must contain theSameElementsAs List(activeEntries(1), activeEntries(3)))
  }

  it should "apply filtering of entries using keys and properties" in withCorrespondenceListEntries(
    correspondenceListEntries
  ) { _ =>
    repository
      .fetchEntries(
        E200,
        filterKeys = Some(Set("27101944")),
        filterProperties = Some(Map("actionIdentification" -> JsString("437"))),
        activeAt = Instant.parse("2025-06-05T00:00:00Z"),
        None,
        None
      )
      .map(_ must contain only activeEntries(1))
  }

  "CorrespondenceListsRepository.executeInstructions" should "invalidate existing entries" in withCorrespondenceListEntries(
    activeEntries :+ activeEntry
  ) { session =>
    for {
      _ <- repository
        .executeInstructions(
          session,
          List(
            InvalidateEntry(
              activeEntry.copy(activeFrom = Instant.parse("2025-05-22T00:00:00Z"))
            )
          )
        )

      entries <- repository.collection
        .find(
          session,
          and(
            Filters.eq("codeListCode", E200.code),
            Filters.eq("key", activeEntry.key),
            Filters.eq("value", activeEntry.value)
          )
        )
        .toFuture()

    } yield entries mustBe Seq(
      activeEntry.copy(activeTo = Some(Instant.parse("2025-05-22T00:00:00Z")))
    )
  }

  it should "invalidate existing entries when the activation date is the same as the existing record" in withCorrespondenceListEntries(
    activeEntries :+ activeEntry
  ) { session =>
    for {
      _ <- repository
        .executeInstructions(
          session,
          List(InvalidateEntry(activeEntry))
        )

      entries <- repository.collection
        .find(
          session,
          and(
            Filters.eq("codeListCode", E200.code),
            Filters.eq("key", activeEntry.key),
            Filters.eq("value", activeEntry.value)
          )
        )
        .toFuture()

    } yield entries mustBe Seq(activeEntry.copy(activeTo = Some(activeEntry.activeFrom)))
  }

  it should "invalidate missing entries" in withCorrespondenceListEntries(
    activeEntries :+ activeEntry
  ) { session =>
    for {
      _ <- repository
        .executeInstructions(
          session,
          List(
            RecordMissingEntry(
              E200,
              activeEntry.key,
              activeEntry.value,
              Instant.parse("2025-05-22T00:00:00Z")
            )
          )
        )

      entries <- repository.collection
        .find(
          session,
          and(
            Filters.eq("codeListCode", E200.code),
            Filters.eq("key", activeEntry.key),
            Filters.eq("value", activeEntry.value)
          )
        )
        .toFuture()

    } yield entries mustBe Seq(
      activeEntry.copy(activeTo = Some(Instant.parse("2025-05-22T00:00:00Z")))
    )
  }

  it should "invalidate missing entries when the activation date is the same as the existing one" in withCorrespondenceListEntries(
    activeEntries :+ activeEntry
  ) { session =>
    for {
      _ <- repository
        .executeInstructions(
          session,
          List(
            RecordMissingEntry(
              E200,
              activeEntry.key,
              activeEntry.value,
              activeEntry.activeFrom
            )
          )
        )

      entries <- repository.collection
        .find(
          session,
          and(
            Filters.eq("codeListCode", E200.code),
            Filters.eq("key", activeEntry.key),
            Filters.eq("value", activeEntry.value)
          )
        )
        .toFuture()

    } yield entries mustBe Seq(activeEntry.copy(activeTo = Some(activeEntry.activeFrom)))
  }

  it should "supersede and invalidate existing entries" in withCorrespondenceListEntries(
    activeEntries :+ activeEntry
  ) { session =>
    val newActiveFrom = Instant.parse("2024-02-01T00:00:00Z")

    for {
      _ <- repository
        .executeInstructions(
          session,
          List(UpsertEntry(activeEntry.copy(activeFrom = newActiveFrom)))
        )

      entries <- repository.collection
        .find(
          session,
          and(
            Filters.eq("codeListCode", E200.code),
            Filters.eq("key", activeEntry.key),
            Filters.eq("value", activeEntry.value)
          )
        )
        .sort(Sorts.ascending("activeFrom"))
        .toFuture()

    } yield entries mustBe Seq(
      activeEntry.copy(activeTo = Some(newActiveFrom)),
      activeEntry.copy(activeFrom = newActiveFrom)
    )
  }

  it should "create a new entry when a new key->value mapping is encountered" in withCorrespondenceListEntries(
    activeEntries
  ) { session =>
    for {
      _ <- repository
        .executeInstructions(
          session,
          List(UpsertEntry(newCorrespondenceEntry))
        )

      entries <- repository.collection
        .find(
          session,
          and(
            Filters.eq("codeListCode", E200.code),
            Filters.eq("key", newCorrespondenceEntry.key)
          )
        )
        .toFuture()

    } yield entries mustBe Seq(newCorrespondenceEntry)
  }

  it should "replace existing entries with same active from date" in withCorrespondenceListEntries(
    activeEntries :+ activeEntry
  ) { session =>
    val updatedEntry = activeEntry.copy(
      properties = Json.obj(
        "actionIdentification" -> "2499"
      )
    )

    for {
      _ <- repository
        .executeInstructions(
          session,
          List(UpsertEntry(updatedEntry))
        )

      entries <- repository.collection
        .find(
          session,
          and(
            Filters.eq("codeListCode", E200.code),
            Filters.eq("key", activeEntry.key)
          )
        )
        .sort(Sorts.ascending("activeFrom"))
        .toFuture()

    } yield entries mustBe Seq(updatedEntry)
  }

  it should "upsert entries in the order of their active from date" in {
    repository.withSessionAndTransaction { session =>
      for {
        _ <- repository
          .executeInstructions(
            session,
            List(
              InvalidateEntry(
                activeEntry.copy(activeFrom = Instant.parse("2025-05-22T00:00:00Z"))
              ),
              UpsertEntry(postDatedE470Entry),
              RecordMissingEntry(
                E200,
                newCorrespondenceEntry.key,
                newCorrespondenceEntry.value,
                Instant.parse("2025-01-17T00:00:00Z")
              ),
              UpsertEntry(existingE470Entry),
              UpsertEntry(newCorrespondenceEntry)
            )
          )

        entries <- repository.collection
          .find(
            session,
            and(
              Filters.eq("codeListCode", E200.code)
            )
          )
          .sort(Sorts.ascending("key", "value", "activeFrom"))
          .toFuture()

      } yield entries mustBe Seq(
        newCorrespondenceEntry.copy(activeTo = Some(Instant.parse("2025-01-17T00:00:00Z"))),
        existingE470Entry.copy(activeTo = Some(postDatedE470Entry.activeFrom)),
        postDatedE470Entry
      )
    }.futureValue
  }
  "CorrespondenceListsRepository.saveCorrespondenceListEntries" should "save the provided codelist entries" in {
    val newEntries = List(
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
      ),
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
      ),
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
      ),
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

    repository.withSessionAndTransaction { session =>
      repository.saveEntries(session, E200, newEntries)
    }.futureValue

    find(
      Filters.equal("codeListCode", E200.code)
    ).futureValue must contain theSameElementsAs newEntries
  }

  it should "remove existing entries when new entries are saved" in {
    val existingEntries = List(
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
      ),
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
    )

    repository.collection.insertMany(existingEntries).toFuture().futureValue

    val newEntries = List(
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
      ),
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

    repository.withSessionAndTransaction { session =>
      repository.saveEntries(session, E200, newEntries)
    }.futureValue

    find(
      Filters.equal("codeListCode", E200.code)
    ).futureValue must contain theSameElementsAs newEntries
  }
}
