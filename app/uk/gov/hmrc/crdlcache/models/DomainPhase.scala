/*
 * Copyright 2026 HM Revenue & Customs
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

package uk.gov.hmrc.crdlcache.models

import play.api.mvc.QueryStringBindable

case class DomainPhase(domain: Domain, phase: Phase)

object DomainPhase {
  val NCTS5: DomainPhase = DomainPhase(Domain.NCTS, Phase.NCTS5)
  val NCTS6: DomainPhase = DomainPhase(Domain.NCTS, Phase.NCTS6)

  val Excise41: DomainPhase = DomainPhase(Domain.Excise, Phase.Excise41)
  val Excise42: DomainPhase = DomainPhase(Domain.Excise, Phase.Excise42)

  val VPD2026: DomainPhase = DomainPhase(Domain.VPD, Phase.VPD2026)

  val REX: DomainPhase = DomainPhase(Domain.REX, Phase.REX)

  given domainPhaseBindable(using
    domainBindable: QueryStringBindable[Domain],
    phaseBindable: QueryStringBindable[Phase]
  ): QueryStringBindable[DomainPhase] = new QueryStringBindable[DomainPhase] {
    override def unbind(key: String, value: DomainPhase): String =
      Seq(
        domainBindable.unbind("domain", value.domain),
        phaseBindable.unbind("phase", value.phase)
      ).mkString("&")

    override def bind(
      key: String,
      params: Map[String, Seq[String]]
    ): Option[Either[String, DomainPhase]] = {
      val maybeDomain = domainBindable.bind("domain", params)
      val maybePhase  = phaseBindable.bind("phase", params)

      (maybeDomain, maybePhase) match {
        case (Some(domainOrError), Some(phaseOrError)) =>
          val domainPhase = for {
            domain <- domainOrError
            phase  <- phaseOrError
          } yield DomainPhase(domain, phase)

          Some(domainPhase)

        case (Some(_), None) | (None, Some(_)) =>
          Some(
            Left("Both phase and domain must be provided together, or neither should be provided")
          )

        case _ =>
          None
      }
    }
  }
}
