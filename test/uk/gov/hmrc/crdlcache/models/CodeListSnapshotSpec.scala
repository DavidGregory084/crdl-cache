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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import play.api.libs.json.Json
import uk.gov.hmrc.crdlcache.config.CodeListConfig
import uk.gov.hmrc.crdlcache.models.CodeListCode.BC08
import uk.gov.hmrc.crdlcache.models.CodeListOrigin.SEED
import uk.gov.hmrc.crdlcache.models.Operation.Update
import uk.gov.hmrc.crdlcache.models.dps.codelist.DpsCodeListSnapshot

import java.time.Instant

class CodeListSnapshotSpec extends AnyFlatSpec with Matchers with TestData {
  private val BC08Config = CodeListConfig(BC08, SEED, "CountryCode")

  "CodeListSnapshot.fromDpsSnapshot" should "convert a DPS codelist snapshot to the crdl-cache model" in {
    val inputSnapshot = DpsCodeListSnapshot(BC08, "Country", 1, List(BC08Aruba))

    val expectedSnapshot = CodeListSnapshot(
      BC08,
      "Country",
      1,
      Set(
        CodeListSnapshotEntry(
          "AW",
          "Aruba",
          Instant.parse("2024-01-17T00:00:00Z"),
          Some(Instant.parse("2024-01-17T00:00:00Z")),
          Some(Update),
          None,
          Json.obj(
            "actionIdentification" -> "811"
          ),
          None,
          None
        )
      )
    )

    CodeListSnapshot.fromDpsSnapshot(BC08Config, inputSnapshot) mustBe expectedSnapshot
  }
}
