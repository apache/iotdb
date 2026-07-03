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

#include "SessionDataSet.h"
#include "TableSession.h"
#include "TableSessionBuilder.h"

int main() {
  TableSessionBuilder builder;
  builder.host("127.0.0.1")->rpcPort(6667)->username("root")->password("root")->useSSL(false);
  std::shared_ptr<TableSession> session = builder.build();
  session->open();

  session->executeNonQueryStatement("DROP DATABASE IF EXISTS cpp_demo_table");
  session->executeNonQueryStatement("CREATE DATABASE cpp_demo_table");
  session->executeNonQueryStatement("USE cpp_demo_table");
  session->executeNonQueryStatement(
      "CREATE TABLE IF NOT EXISTS demo_t (tag1 STRING TAG, value INT32 FIELD)");
  session->executeNonQueryStatement("INSERT INTO demo_t(time, tag1, value) VALUES (1, 'a', 42)");

  std::unique_ptr<SessionDataSet> dataSet(
      session->executeQueryStatement("SELECT time, value FROM demo_t WHERE tag1 = 'a'"));
  if (!dataSet || !dataSet->hasNext()) {
    std::cerr << "[cpp_table_example] expected one row\n";
    return 1;
  }
  std::shared_ptr<RowRecord> record = dataSet->next();
  if (record->fields[1].intV.value() != 42) {
    std::cerr << "[cpp_table_example] unexpected value\n";
    return 1;
  }
  dataSet->closeOperationHandle();

  session->executeNonQueryStatement("DROP DATABASE IF EXISTS cpp_demo_table");
  session->close();
  std::cout << "[cpp_table_example] ok\n";
  return 0;
}
