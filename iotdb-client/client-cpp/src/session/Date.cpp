/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "Date.h"

#include <climits>
#include <cstdio>
#include <string>

#include "Common.h"

std::string IoTDBDate::toIsoExtendedString() const {
  if (!valid_) {
    return "";
  }
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year_, month_, day_);
  return std::string(buf);
}

int32_t parseDateExpressionToInt(const IoTDBDate& date) {
  if (date.is_not_a_date()) {
    throw IoTDBException("Date expression is null or empty.");
  }

  const int year = date.year();
  if (year < 1000 || year > 9999) {
    throw DateTimeParseException("Year must be between 1000 and 9999.", date.toIsoExtendedString(),
                                 0);
  }

  const int64_t result = static_cast<int64_t>(year) * 10000 + date.month() * 100 + date.day();
  if (result > INT32_MAX || result < INT32_MIN) {
    throw DateTimeParseException("Date value overflow. ", date.toIsoExtendedString(), 0);
  }
  return static_cast<int32_t>(result);
}

IoTDBDate parseIntToDate(int32_t dateInt) {
  const int year = dateInt / 10000;
  const int month = (dateInt % 10000) / 100;
  const int day = dateInt % 100;
  return IoTDBDate(year, month, day);
}
