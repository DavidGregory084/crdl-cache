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

package uk.gov.hmrc.crdlcache.models.errors

enum ImportError(val message: String, val cause: Throwable = null)
  extends Exception(message, cause) {

  case RequiredDataItemMissing(itemName: String, listKey: String)
    extends ImportError(
      s"A data item with name '$itemName' is required, but missing from a reference data entry in list '$listKey'"
    )

  case RequiredDataItemsMissing(listKey: String, itemNames: String*)
    extends ImportError(
      s"A data item with one of the following names is required, but missing from a reference data entry in list '$listKey': ${itemNames.mkString("'", "', '", "'")}"
    )

  case UnknownState(code: String)
    extends ImportError(
      s"An unknown reference data state '$code' was found in a CS/RD2 reference data entry"
    )

  case UnknownOperation(code: String)
    extends ImportError(
      s"An unknown reference data operation '$code' was found in a SEED reference data entry"
    )

  case LanguageDescriptionMissing
    extends ImportError(
      "Language description for language code 'en' was missing from a reference data entry"
    )

  case CustomsOfficeDetailMissing(referenceNumber: String)
    extends ImportError(
      s"CustomsOfficeDetail was missing from customs office data from referenceNumber: '$referenceNumber'"
    )

  case InvalidDateFormat(invalidDate: String)
    extends ImportError(s"An error occurred while parsing the date value: $invalidDate")

  case InvalidTimeFormat(invalidTime: String)
    extends ImportError(s"An error occurred while parsing time value: $invalidTime")

  case InvalidDayFormat(invalidDay: String)
    extends ImportError(s"An error occurred while parsing day value: $invalidDay")
}
