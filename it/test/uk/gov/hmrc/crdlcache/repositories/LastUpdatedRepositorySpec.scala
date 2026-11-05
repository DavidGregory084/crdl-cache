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

import org.mongodb.scala.ClientSession
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Assertion, OptionValues}
import uk.gov.hmrc.crdlcache.models.CodeListCode.{BC08, BC66, CL251}
import uk.gov.hmrc.crdlcache.models.{CodeListCode, Domain, LastUpdated, Phase}
import uk.gov.hmrc.mongo.test.{
  CleanMongoCollectionSupport,
  IndexedMongoQueriesSupport,
  PlayMongoRepositorySupport
}
import uk.gov.hmrc.mongo.transaction.TransactionConfiguration

import java.time.{Clock, ZoneOffset}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class LastUpdatedRepositorySpec
  extends AnyFlatSpec
  with PlayMongoRepositorySupport[LastUpdated]
  with CleanMongoCollectionSupport
  with IndexedMongoQueriesSupport
  with Matchers
  with OptionValues
  with ScalaFutures {

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 30.seconds, interval = 100.millis)

  private val clock = Clock.tickMillis(ZoneOffset.UTC)

  given TransactionConfiguration = TransactionConfiguration.strict
  given ExecutionContext         = ExecutionContext.global

  override protected val repository: LastUpdatedRepository = new LastUpdatedRepository(
    mongoComponent
  )

  private def withSession(test: ClientSession => Future[Assertion]): Unit =
    repository.withClientSession(test).futureValue

  "LastUpdatedRepository.fetchLastUpdated" should "return None when no last updated date has been populated" in {
    repository.fetchLastUpdated(BC08, None, None).futureValue mustBe None
  }

  "LastUpdatedRepository.setLastUpdated" should "upsert the last updated date when it is not present" in withSession {
    session =>
      val inputInstant = clock.instant()
      for {
        _           <- repository.setLastUpdated(session, BC08, 1, None, None, inputInstant)
        lastUpdated <- repository.fetchLastUpdated(BC08, None, None)
      } yield lastUpdated mustBe Some(LastUpdated(BC08, 1, None, None, inputInstant))
  }

  "LastUpdatedRepository.setLastUpdated" should "upsert the value for codelists with phase and domain" in withSession {
    session =>
      val inputInstant = clock.instant()
      for {
        _           <- repository.setLastUpdated(session, CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), inputInstant)
        lastUpdated <- repository.fetchLastUpdated(CL251, None, None)
      } yield lastUpdated mustBe Some(LastUpdated(CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), inputInstant))
  }

  it should "only upsert the value for the specified codelist" in withSession { session =>
    val inputInstant = clock.instant()
    for {
      _           <- repository.setLastUpdated(session, BC08, 1, None, None, inputInstant)
      lastUpdated <- repository.fetchLastUpdated(BC66, None, None)
    } yield lastUpdated mustBe None
  }

  it should "return the latest value when updated multiple times" in withSession { session =>
    val instant1 = clock.instant()
    val instant2 = instant1.plusSeconds(20)
    for {
      _           <- repository.setLastUpdated(session, BC08, 1, None, None, instant1)
      _           <- repository.setLastUpdated(session, BC08, 2, None, None, instant2)
      lastUpdated <- repository.fetchLastUpdated(BC08, None, None)
    } yield lastUpdated mustBe Some(LastUpdated(BC08, 2, None, None, instant2))
  }

  it should "return the latest value only for the specified codelist" in withSession { session =>
    val instant1 = clock.instant()
    val instant2 = instant1.plusSeconds(20)
    for {
      _            <- repository.setLastUpdated(session, BC08, 1, None, None, instant1)
      _            <- repository.setLastUpdated(session, BC66, 1, None, None, instant2)
      lastUpdated1 <- repository.fetchLastUpdated(BC08, None, None)
      lastUpdated2 <- repository.fetchLastUpdated(BC66, None, None)
    } yield {
      lastUpdated1 mustBe Some(LastUpdated(BC08, 1, None, None, instant1))
      lastUpdated2 mustBe Some(LastUpdated(BC66, 1, None, None, instant2))
    }
  }

  it should "return the latest last updated value only for all the codelists" in withSession {
    session =>
      val instant1 = clock.instant()
      val instant2 = instant1.plusSeconds(20)
      for {
        _           <- repository.setLastUpdated(session, BC08, 1, None, None, instant1)
        _           <- repository.setLastUpdated(session, BC08, 2, None, None, instant2)
        _           <- repository.setLastUpdated(session, BC66, 1, None, None, instant2)
        lastUpdated <- repository.fetchAllLastUpdated
      } yield {
        lastUpdated mustBe List(LastUpdated(BC08, 2, None, None, instant2), LastUpdated(BC66, 1, None, None, instant2))
      }
  }

  "LastUpdatedRepository.fetchAllLastUpdatedPaged" should "return all codelists when no filters are provided" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _           <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _           <- repository.setLastUpdated(session, BC66, 1, None, None, instant)
        lastUpdated <- repository.fetchAllLastUpdatedPaged(1, 10)
      } yield {
        lastUpdated mustBe List(LastUpdated(BC08, 1, None, None, instant), LastUpdated(BC66, 1, None, None, instant))
      }
  }

  it should "filter by codeListCode using a case-insensitive partial match" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _           <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _           <- repository.setLastUpdated(session, BC66, 1, None, None, instant)
        _           <- repository.setLastUpdated(session, CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant)
        lastUpdated <- repository.fetchAllLastUpdatedPaged(1, 10, codeListCode = Some(CodeListCode("bc")))
      } yield {
        lastUpdated mustBe List(LastUpdated(BC08, 1, None, None, instant), LastUpdated(BC66, 1, None, None, instant))
      }
  }

  it should "filter by phase using an exact match" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _           <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _           <- repository.setLastUpdated(session, CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant)
        lastUpdated <- repository.fetchAllLastUpdatedPaged(1, 10, phase = Some(Phase.NCTS6))
      } yield {
        lastUpdated mustBe List(LastUpdated(CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant))
      }
  }

  it should "filter by domain using an exact match" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _           <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _           <- repository.setLastUpdated(session, CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant)
        lastUpdated <- repository.fetchAllLastUpdatedPaged(1, 10, domain = Some(Domain.NCTS))
      } yield {
        lastUpdated mustBe List(LastUpdated(CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant))
      }
  }

  it should "return a paged subset of results" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _           <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _           <- repository.setLastUpdated(session, BC66, 1, None, None, instant)
        lastUpdated <- repository.fetchAllLastUpdatedPaged(1, 1)
      } yield {
        lastUpdated mustBe List(LastUpdated(BC08, 1, None, None, instant))
      }
  }

  "LastUpdatedRepository.codeListCount" should "return the total count when no filters are provided" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _     <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _     <- repository.setLastUpdated(session, BC66, 1, None, None, instant)
        count <- repository.codeListCount()
      } yield {
        count mustBe 2L
      }
  }

  it should "return a filtered count when codeListCode is provided" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _     <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _     <- repository.setLastUpdated(session, BC66, 1, None, None, instant)
        _     <- repository.setLastUpdated(session, CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant)
        count <- repository.codeListCount(codeListCode = Some(CodeListCode("BC")))
      } yield {
        count mustBe 2L
      }
  }

  it should "return a filtered count when phase is provided" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _     <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _     <- repository.setLastUpdated(session, CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant)
        count <- repository.codeListCount(phase = Some(Phase.NCTS6))
      } yield {
        count mustBe 1L
      }
  }

  it should "return a filtered count when domain is provided" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _     <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        _     <- repository.setLastUpdated(session, CL251, 1, Some(Phase.NCTS6), Some(Domain.NCTS), instant)
        count <- repository.codeListCount(domain = Some(Domain.NCTS))
      } yield {
        count mustBe 1L
      }
  }

  it should "return zero when no codelists match the filters" in withSession {
    session =>
      val instant = clock.instant()
      for {
        _     <- repository.setLastUpdated(session, BC08, 1, None, None, instant)
        count <- repository.codeListCount(codeListCode = Some(CodeListCode("XYZ")))
      } yield {
        count mustBe 0L
      }
  }
}
