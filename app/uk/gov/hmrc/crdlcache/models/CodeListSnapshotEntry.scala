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

import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json.{JsObject, Json}
import uk.gov.hmrc.crdlcache.config.{
  CodeListConfig,
  CorrespondenceListConfig,
  ListConfig,
  PhaseAndDomainListConfig
}
import uk.gov.hmrc.crdlcache.models.CodeListCode.code
import uk.gov.hmrc.crdlcache.models.CodeListOrigin.{CSRD2, SEED}
import uk.gov.hmrc.crdlcache.models.dps.codelist.DpsCodeListEntry
import uk.gov.hmrc.crdlcache.models.errors.ImportError.*
import uk.gov.hmrc.crdlcache.utils.ParserUtils.parseDateToInstant

import java.time.Instant
import java.time.format.DateTimeFormatter

case class CodeListSnapshotEntry(
  key: String,
  value: String,
  activeFrom: Instant,
  updatedAt: Option[Instant],
  operation: Option[Operation],
  state: Option[State],
  properties: JsObject,
  phase: Option[Phase],
  domain: Option[Domain]
)

object CodeListSnapshotEntry {
  private val dateFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy[ HH:mm:ss]")

  private val knownProperties = Set(
    SEED.activeDateProperty,
    SEED.modificationDateProperty.get,
    SEED.operationProperty.get,
    // TODO: Add this if we can remove state=valid filtering from customs-reference-data.
    //       It will remove this property from the codelist entry because validity is handled by the import job.
    // CSRD2.stateProperty.get,
    CSRD2.activeDateProperty
  )

  def fromDpsEntry(
    config: ListConfig,
    dpsEntry: DpsCodeListEntry
  ): CodeListSnapshotEntry = {
    val key = dpsEntry
      .getProperty(config.keyProperty)
      .flatMap(_.dataitem_value)
      .getOrElse(throw RequiredDataItemMissing(config.keyProperty, config.code.code))

    val value = config match {
      case _: CodeListConfig | _: PhaseAndDomainListConfig =>
        dpsEntry.language
          .find(_.lang_code.equalsIgnoreCase("en"))
          .map(_.lang_desc)
          .getOrElse(throw LanguageDescriptionMissing)
      case correspondence: CorrespondenceListConfig =>
        dpsEntry
          .getProperty(correspondence.valueProperty)
          .flatMap(_.dataitem_value)
          .getOrElse(throw RequiredDataItemMissing(correspondence.valueProperty, config.code.code))
    }

    val activeFrom = dpsEntry
      .getProperty(config.origin.activeDateProperty)
      .flatMap(_.dataitem_value)
      .map(parseDateToInstant(_, dateFormat))
      .getOrElse(
        throw RequiredDataItemsMissing(
          config.code.code,
          CodeListOrigin.values.map(_.activeDateProperty)*
        )
      )

    val updatedAt = config.origin.modificationDateProperty
      .flatMap(dpsEntry.getProperty)
      .flatMap(_.dataitem_value)
      .map(parseDateToInstant(_, dateFormat))

    val operation = config.origin.operationProperty
      .flatMap(dpsEntry.getProperty)
      .flatMap(_.dataitem_value)
      .map { value => Operation.fromString(value).getOrElse(throw UnknownOperation(value)) }

    val state = config.origin.stateProperty
      .flatMap(dpsEntry.getProperty)
      .flatMap(_.dataitem_value)
      .map { value => State.fromString(value).getOrElse(throw UnknownState(value)) }

    val usedProperties =
      config match {
        case _: CodeListConfig | _: PhaseAndDomainListConfig =>
          knownProperties.incl(config.keyProperty)
        case correspondence: CorrespondenceListConfig =>
          knownProperties.incl(config.keyProperty).incl(correspondence.valueProperty)
      }

    val builder = Json.newBuilder

    dpsEntry.dataitem
      .filterNot(item => usedProperties.contains(item.dataitem_name))
      .filterNot(item => item.dataitem_value.isEmpty)
      .foreach { item =>
        val propertyValue: JsValueWrapper =
          if (item.propertyName.endsWith("Flag")) item.dataitem_value.contains("1")
          else item.dataitem_value.orNull
        builder += (item.propertyName -> propertyValue)
      }

    val phase: Option[Phase] = None

    val domain: Option[Domain] = None

    CodeListSnapshotEntry(
      key,
      value,
      activeFrom,
      updatedAt,
      operation,
      state,
      builder.result(),
      phase,
      domain
    )
  }
}
