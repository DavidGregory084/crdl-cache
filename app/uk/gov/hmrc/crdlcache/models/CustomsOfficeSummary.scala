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

package uk.gov.hmrc.crdlcache.models

import play.api.libs.functional.syntax.*
import play.api.libs.json.*

final case class CustomsOfficeSummary(
  referenceNumber: String,
  countryCode: String,
  customsOfficeUsualName: String,
  phase: Option[Phase],
  domain: Option[Domain]
)

object CustomsOfficeSummary {
  given reads: Reads[CustomsOfficeSummary] = (
    (__ \ "referenceNumber").read[String] and
      (__ \ "countryCode").read[String] and
      (__ \ "customsOfficeLsd" \ "customsOfficeUsualName").read[String] and
      (__ \ "phase").readNullable[Phase] and
      (__ \ "domain").readNullable[Domain]
  )(CustomsOfficeSummary.apply _)

  given writes: Writes[CustomsOfficeSummary] = Json.writes[CustomsOfficeSummary]

  given format: Format[CustomsOfficeSummary] = Format(reads, writes)
}
