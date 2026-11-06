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

enum CodeListOrigin {
  case SEED
  case CSRD2

  def activeDateProperty: String = this match {
    case SEED  => "Action_ActivationDate"
    case CSRD2 => "RDEntryStatus_activeFrom"
  }

  def modificationDateProperty: Option[String] = this match {
    case SEED  => Some("Action_ModificationDateAndTime")
    case CSRD2 => None
  }

  def stateProperty: Option[String] = this match {
    case CSRD2 => Some("RDEntryStatus_state")
    case SEED  => None
  }

  def operationProperty: Option[String] = this match {
    case SEED  => Some("Action_Operation")
    case CSRD2 => None
  }
}
