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

import java.time.Instant

enum Instruction {
  case UpsertEntry(codeListEntry: CodeListEntry)
  case InvalidateEntry(codeListEntry: CodeListEntry)
  // case DeleteEntry(codeListEntry: CodeListEntry)
  case RecordMissingEntry(
    codeListCode: CodeListCode,
    override val key: String,
    phase: Option[Phase],
    domain: Option[Domain],
    removedAt: Instant
  )

  def key: String = this match {
    case Instruction.UpsertEntry(codeListEntry)     => codeListEntry.key
    case Instruction.InvalidateEntry(codeListEntry) => codeListEntry.key
    // case Instruction.DeleteEntry(codeListEntry)     => codeListEntry.key
    case Instruction.RecordMissingEntry(_, key, _, _, _) => key
  }

  def activeFrom: Instant = this match {
    case Instruction.UpsertEntry(codeListEntry)     => codeListEntry.activeFrom
    case Instruction.InvalidateEntry(codeListEntry) => codeListEntry.activeFrom
    // case Instruction.DeleteEntry(codeListEntry)     => codeListEntry.activeFrom
    case Instruction.RecordMissingEntry(_, _, _, _, removedAt) => removedAt
  }
}
