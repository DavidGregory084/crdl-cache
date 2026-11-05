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
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.{ActorAttributes, DelayOverflowStrategy, Supervision}
import org.mongodb.scala.ClientSession
import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext}
import play.api.Logging
import uk.gov.hmrc.crdlcache.config.AppConfig
import uk.gov.hmrc.crdlcache.connectors.DpsConnector
import uk.gov.hmrc.crdlcache.models.CustomsOfficeListsInstruction.{
  RecordMissingCustomsOffice,
  UpsertCustomsOffice
}
import uk.gov.hmrc.crdlcache.models.{CustomsOffice, CustomsOfficeListsInstruction, Domain, Phase}
import uk.gov.hmrc.crdlcache.repositories.CustomsOfficeListsRepository
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.lock.{LockService, MongoLockRepository}
import uk.gov.hmrc.mongo.transaction.{TransactionConfiguration, Transactions}

import java.time.{Clock, Instant, LocalDate, ZoneOffset}
import javax.inject.Inject
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

@DisallowConcurrentExecution
class ImportCustomsOfficesListJob @Inject() (
  val mongoComponent: MongoComponent,
  val lockRepository: MongoLockRepository,
  customsOfficeListsRepository: CustomsOfficeListsRepository,
  dpsConnector: DpsConnector,
  appConfig: AppConfig,
  clock: Clock
)(using system: ActorSystem, ec: ExecutionContext)
  extends Job
  with LockService
  with Logging
  with Transactions {

  protected val phase: Option[Phase]   = appConfig.importOfficesJobPhase
  protected val domain: Option[Domain] = appConfig.importOfficesJobDomain

  given TransactionConfiguration = TransactionConfiguration.strict

  val lockId: String = "import-offices"
  val ttl: Duration  = 1.hour

  private[schedulers] def processCustomsOffices(
    session: ClientSession,
    customsOffices: List[CustomsOffice],
    phase: Option[Phase] = None,
    domain: Option[Domain] = None
  ): Future[List[CustomsOfficeListsInstruction]] = {
    logger.info("Importing customs office lists")
    (phase, domain) match {
      case (Some(_), None) | (None, Some(_)) =>
        throw IllegalArgumentException(
          "Both phase and domain must be provided together, or neither should be provided"
        )
      case _ =>
        customsOfficeListsRepository.fetchCustomsOfficeReferenceNumbers(session).flatMap {
          existingReferenceNumbers =>
            val incomingCustomsOffices   = customsOffices.groupBy(_.referenceNumber)
            val incomingReferenceNumbers = incomingCustomsOffices.keySet
            val mergedReferenceNumbers   = existingReferenceNumbers.union(incomingReferenceNumbers)

            val instructions = List.newBuilder[CustomsOfficeListsInstruction]

            for (referenceNumber <- mergedReferenceNumbers) {

              val hasExistingOffice = existingReferenceNumbers.contains(referenceNumber)
              val maybeNewOffice = incomingCustomsOffices.get(referenceNumber).flatMap(_.headOption)

              (hasExistingOffice, maybeNewOffice) match {
                // First case is when there may or may not be an existing office with the reference number and new office details are available, we would Upsert.
                case (_, Some(newOffice)) =>
                  val pDOffice = (phase, domain) match {
                    case (Some(phase), Some(domain)) =>
                      newOffice.copy(phase = Some(phase), domain = Some(domain))
                    case _ =>
                      newOffice
                  }
                  instructions += UpsertCustomsOffice(pDOffice)
                // Second case is when we have an existing office and don't have new office details, we would need to mark this reference number as missing and expire it.
                case (true, None) =>
                  val startOfToday = LocalDate.now(clock).atStartOfDay(ZoneOffset.UTC)
                  instructions += RecordMissingCustomsOffice(
                    referenceNumber,
                    startOfToday.toInstant
                  )
                case (false, None) =>
                  throw IllegalStateException(
                    "Impossible case - we have neither a new nor existing entry for a key"
                  )
              }
            }
            Future.successful(instructions.result())
        }
    }
  }

  private[schedulers] def importCustomsOfficeLists(): Future[Unit] = {
    val job = for {
      preIngestOfficeCount <- customsOfficeListsRepository.customsOfficesCount(
        Instant.now(),
        domain = Some(Domain.NCTS),
        phase = Some(Phase.NCTS6)
      )
      _ = logger.warn(s"Number of NCTS P6 Customs Offices pre-ingest: ${preIngestOfficeCount}")

      customOfficeLists <- dpsConnector
        .fetchCustomsOfficeLists(phase, domain)
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .mapConcat(_.elements)
        .map(customsOfficeList =>
          CustomsOffice.fromDpsCustomOfficeList(customsOfficeList, phase, domain)
        )
        .withAttributes(ActorAttributes.withSupervisionStrategy { err =>
          logger.error(
            s"Stopping customs office list import job due to exception",
            err
          )
          Supervision.stop
        })
        .runWith(Sink.seq)
      _ <- withSessionAndTransaction { session =>
        for {
          instructions <- processCustomsOffices(session, customOfficeLists.toList)
          _            <- customsOfficeListsRepository.executeInstructions(session, instructions)
        } yield ()

      }
    } yield ()
    job.map { _ =>
      logger.info("Import customs offices job completed successfully")
    }
  }

  def execute(
    context: JobExecutionContext
  ): Unit =
    Await.result(
      withLock(importCustomsOfficeLists()).map {
        _.getOrElse { logger.info("Import Customs offices job lock could not be obtained") }
      },
      Duration.Inf
    )
}
