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
#include "Column.h"

// Inference output may be FLOAT in TsBlock while schema declares DOUBLE.
TEST_CASE("Read float column as double for schema mismatch", "[column][inference]") {
  std::vector<bool> valueIsNull(1, false);
  std::vector<float> values = {120.00000762939453f};
  auto floatColumn = std::make_shared<FloatColumn>(0, 1, valueIsNull, values);
  auto rleColumn = std::make_shared<RunLengthEncodedColumn>(floatColumn, 20);

  REQUIRE(rleColumn->getDataType() == TSDataType::FLOAT);
  REQUIRE_THROWS_AS(rleColumn->getDouble(0), IoTDBException);

  double asDouble = static_cast<double>(rleColumn->getFloat(0));
  REQUIRE(asDouble == Approx(120.0).margin(0.01));
}
