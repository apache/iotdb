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
#ifndef IOTDB_DATE_H
#define IOTDB_DATE_H

#include <cstdint>
#include <string>

constexpr int32_t EMPTY_DATE_INT = 10000101;

class IoTDBDate {
public:
  IoTDBDate() : valid_(false), year_(0), month_(0), day_(0) {}

  IoTDBDate(int year, int month, int day) : valid_(true), year_(year), month_(month), day_(day) {}

  static IoTDBDate notADate() {
    return IoTDBDate();
  }

  bool is_not_a_date() const {
    return !valid_;
  }

  int year() const {
    return year_;
  }

  int month() const {
    return month_;
  }

  int day() const {
    return day_;
  }

  std::string toIsoExtendedString() const;

  friend bool operator==(const IoTDBDate& lhs, const IoTDBDate& rhs) {
    return lhs.valid_ == rhs.valid_ && lhs.year_ == rhs.year_ && lhs.month_ == rhs.month_ &&
           lhs.day_ == rhs.day_;
  }

  friend bool operator!=(const IoTDBDate& lhs, const IoTDBDate& rhs) {
    return !(lhs == rhs);
  }

private:
  bool valid_;
  int year_;
  int month_;
  int day_;
};

int32_t parseDateExpressionToInt(const IoTDBDate& date);
IoTDBDate parseIntToDate(int32_t dateInt);

#endif
