/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "catch.hpp"
#include "Session.h"

#include <memory>
#include <vector>

using namespace std;

TEST_CASE("SessionUtils filterNullColumns keeps only non-null FIELD columns", "[utils]") {
  vector<pair<string, TSDataType::TSDataType>> schemas = {{"s1", TSDataType::INT32},
                                                          {"s2", TSDataType::INT64},
                                                          {"s3", TSDataType::FLOAT},
                                                          {"s4", TSDataType::DOUBLE},
                                                          {"s5", TSDataType::BOOLEAN}};
  Tablet tablet("root.sg.d1", schemas, 1);
  tablet.timestamps[0] = 1000L;
  tablet.rowSize = 1;
  tablet.addValue("s1", 0, 1);
  tablet.addValue("s3", 0, 1.5f);
  tablet.bitMaps[1].mark(0);
  tablet.bitMaps[3].mark(0);
  tablet.bitMaps[4].mark(0);

  std::shared_ptr<const Tablet> filtered = SessionUtils::filterNullColumns(tablet);
  REQUIRE(filtered != nullptr);
  REQUIRE(filtered.get() != &tablet);
  REQUIRE(filtered.use_count() == 1);
  REQUIRE(filtered->schemas.size() == 2);
  REQUIRE(filtered->schemas[0].first == "s1");
  REQUIRE(filtered->schemas[1].first == "s3");
  REQUIRE(((int*)filtered->values[0])[0] == 1);
  REQUIRE(((float*)filtered->values[1])[0] == Approx(1.5f));
}

TEST_CASE("SessionUtils filterNullColumns returns original when nothing to drop", "[utils]") {
  vector<pair<string, TSDataType::TSDataType>> schemas = {{"s1", TSDataType::INT32}};
  Tablet tablet("root.sg.d1", schemas, 1);
  tablet.timestamps[0] = 1L;
  tablet.rowSize = 1;
  tablet.addValue("s1", 0, 1);

  std::shared_ptr<const Tablet> filtered = SessionUtils::filterNullColumns(tablet);
  REQUIRE(filtered.get() == &tablet);
}

TEST_CASE("SessionUtils filterNullColumns returns nullptr when tree-model FIELD columns are all null",
          "[utils]") {
  vector<pair<string, TSDataType::TSDataType>> schemas = {{"s1", TSDataType::INT32},
                                                          {"s2", TSDataType::INT64}};
  Tablet tablet("root.sg.d1", schemas, 1);
  tablet.timestamps[0] = 2000L;
  tablet.rowSize = 1;
  tablet.bitMaps[0].mark(0);
  tablet.bitMaps[1].mark(0);

  std::shared_ptr<const Tablet> filtered = SessionUtils::filterNullColumns(tablet);
  REQUIRE(filtered == nullptr);
}

TEST_CASE("SessionUtils filterNullColumns keeps TAG when table-model FIELD columns are all null",
          "[utils]") {
  vector<pair<string, TSDataType::TSDataType>> schemas = {
      {"tag1", TSDataType::TEXT}, {"s1", TSDataType::INT32}, {"s2", TSDataType::INT64}};
  vector<ColumnCategory> columnTypes = {ColumnCategory::TAG, ColumnCategory::FIELD,
                                        ColumnCategory::FIELD};
  Tablet tablet("table1", schemas, columnTypes, 1);
  tablet.timestamps[0] = 3000L;
  tablet.rowSize = 1;
  tablet.addValue("tag1", 0, string("d1"));
  tablet.bitMaps[1].mark(0);
  tablet.bitMaps[2].mark(0);

  std::shared_ptr<const Tablet> filtered = SessionUtils::filterNullColumns(tablet);
  REQUIRE(filtered != nullptr);
  REQUIRE(filtered.get() != &tablet);
  REQUIRE(filtered->schemas.size() == 1);
  REQUIRE(filtered->schemas[0].first == "tag1");
  REQUIRE(filtered->columnTypes.size() == 1);
  REQUIRE(filtered->columnTypes[0] == ColumnCategory::TAG);
}

TEST_CASE("SessionUtils filterNullColumns checks active rows when bitmap is maxRowNumber-sized",
          "[utils]") {
  vector<pair<string, TSDataType::TSDataType>> schemas = {{"s1", TSDataType::INT32},
                                                          {"s2", TSDataType::INT64}};
  Tablet tablet("root.sg.d1", schemas, 10);
  tablet.timestamps[0] = 1000L;
  tablet.rowSize = 1;
  tablet.addValue("s1", 0, 1);
  tablet.bitMaps[1].mark(0);

  std::shared_ptr<const Tablet> filtered = SessionUtils::filterNullColumns(tablet);
  REQUIRE(filtered != nullptr);
  REQUIRE(filtered->schemas.size() == 1);
  REQUIRE(filtered->schemas[0].first == "s1");
}

TEST_CASE("BitMap isRangeAllMarked matches per-bit scan and rejects OOB", "[utils]") {
  BitMap bitMap(20);
  for (size_t i = 0; i < 20; i++) {
    if (i % 4 != 2) {
      bitMap.mark(i);
    }
  }
  for (size_t start = 0; start <= 20; start++) {
    for (size_t length = 0; length <= 20 - start; length++) {
      bool allMarked = true;
      for (size_t i = start; i < start + length; i++) {
        allMarked = allMarked && bitMap.isMarked(i);
      }
      REQUIRE(bitMap.isRangeAllMarked(start, length) == allMarked);
    }
  }
  REQUIRE(bitMap.isRangeAllMarked(0, 0));
  REQUIRE_FALSE(bitMap.isRangeAllMarked(0, 21));
  REQUIRE_FALSE(bitMap.isRangeAllMarked(21, 0));

  BitMap empty;
  REQUIRE(empty.isAllMarked());
  BitMap full(8);
  full.markAll();
  REQUIRE(full.isAllMarked());
  full.unmark(7);
  REQUIRE_FALSE(full.isAllMarked());
  REQUIRE(full.isRangeAllMarked(0, 7));
}
