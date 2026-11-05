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

opaque type Phase = String

object Phase {
  def apply(value: String): Phase = value

  val Excise41 = Phase("FS4.1")
  val Excise42 = Phase("FS4.2")
  val NCTS5    = Phase("P5")
  val NCTS6    = Phase("P6")
  val REX      = Phase("0")
  val VPD2026  = Phase("HMRC2026")

  given Format[Phase] = Format(Reads.StringReads, Writes.StringWrites)

  given QueryStringBindable[Phase] = QueryStringBindable.bindableString

  extension (phase: Phase) {
    def value: String = phase
  }
}
