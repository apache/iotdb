/*
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

#include <iostream>
#include <memory>

#include "Session.h"
#include "SessionBuilder.h"
#include "SessionDataSet.h"

int main() {
  SessionBuilder builder;
  builder.host("127.0.0.1")->rpcPort(6667)->username("root")->password("root")->useSSL(false);
  std::shared_ptr<Session> session = builder.build();
  session->open(false);

  const std::string database = "root.cpp_demo_tree";
  const std::string timeseries = database + ".d0.s0";
  if (session->checkTimeseriesExists(timeseries)) {
    session->deleteTimeseries(timeseries);
  }
  try {
    session->deleteStorageGroup(database);
  } catch (...) {
  }

  session->setStorageGroup(database);
  session->createTimeseries(timeseries, TSDataType::INT64, TSEncoding::RLE,
                            CompressionType::SNAPPY);
  session->insertRecord(database + ".d0", 1, {"s0"}, {"100"});

  std::unique_ptr<SessionDataSet> dataSet(
      session->executeQueryStatement("SELECT s0 FROM " + database + ".d0"));
  if (!dataSet || !dataSet->hasNext()) {
    std::cerr << "[cpp_tree_example] expected one row\n";
    return 1;
  }
  std::shared_ptr<RowRecord> record = dataSet->next();
  if (record->fields[0].longV.value() != 100) {
    std::cerr << "[cpp_tree_example] unexpected value\n";
    return 1;
  }
  dataSet->closeOperationHandle();

  session->deleteTimeseries(timeseries);
  session->deleteStorageGroup(database);
  session->close();
  std::cout << "[cpp_tree_example] ok\n";
  return 0;
}
