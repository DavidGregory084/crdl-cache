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

import play.api.libs.json.{Format, Reads, Writes}
import play.api.mvc.QueryStringBindable

opaque type Domain = String

object Domain {
  def apply(value: String): Domain = value

  val NCTS: Domain   = Domain("NCTS")
  val Excise: Domain = Domain("Excise")
  val REX: Domain    = Domain("REX")
  val VPD: Domain    = Domain("Excise-VapingDuty")

  given Format[Domain] = Format(Reads.StringReads, Writes.StringWrites)

  given QueryStringBindable[Domain] = QueryStringBindable.bindableString

  extension (domain: Domain) {
    def value: String = domain
  }
}
