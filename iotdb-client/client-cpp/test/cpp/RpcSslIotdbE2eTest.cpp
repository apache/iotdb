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

#include <catch.hpp>

#include <memory>
#include <vector>

#include "Common.h"
#include "ItSslConnection.h"
#include "Session.h"
#include "SessionBuilder.h"
#include "SessionC.h"
#include "SessionDataSet.h"
#include "SslTestFixtures.h"
#include "TableSessionBuilder.h"

#if defined(WITH_SSL) && defined(IOTDB_RPC_SSL_IT)

TEST_CASE("TLS tree Session connects to IoTDB and runs SQL", "[rpc][ssl][iotdb][e2e]") {
  auto session = itssl::newOpenedTreeSession();
  REQUIRE(session != nullptr);

  const std::string database = "root.cpp_ssl_it_tree";
  const std::string timeseries = database + ".d1.s1";
  if (session->checkTimeseriesExists(timeseries)) {
    session->deleteTimeseries(timeseries);
  }
  try {
    session->deleteStorageGroup(database);
  } catch (...) {
  }

  session->setStorageGroup(database);
  session->createTimeseries(timeseries, TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);
  session->insertRecord(database + ".d1", 1, {"s1"}, {"1"});

  std::unique_ptr<SessionDataSet> dataSet(
      session->executeQueryStatement("SELECT s1 FROM " + database + ".d1"));
  REQUIRE(dataSet != nullptr);
  REQUIRE(dataSet->hasNext());
  std::shared_ptr<RowRecord> record = dataSet->next();
  REQUIRE(record != nullptr);
  REQUIRE(record->timestamp == 1);
  REQUIRE(record->fields.size() == 1);
  REQUIRE(record->fields[0].intV.value() == 1);
  REQUIRE_FALSE(dataSet->hasNext());
  dataSet->closeOperationHandle();

  session->deleteTimeseries(timeseries);
  session->deleteStorageGroup(database);
  session->close();
}

TEST_CASE("TLS table Session connects to IoTDB and runs SQL", "[rpc][ssl][iotdb][e2e]") {
  auto session = itssl::newOpenedTableSession();
  REQUIRE(session != nullptr);

  session->executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS cpp_ssl_it_table");
  session->executeNonQueryStatement("USE cpp_ssl_it_table");
  session->executeNonQueryStatement(
      "CREATE TABLE IF NOT EXISTS ssl_it_table (tag1 STRING TAG, value INT32 FIELD)");
  session->executeNonQueryStatement("INSERT INTO ssl_it_table(time, tag1, value) VALUES (1, 't1', 42)");

  std::unique_ptr<SessionDataSet> dataSet(
      session->executeQueryStatement("SELECT time, value FROM ssl_it_table WHERE tag1 = 't1'"));
  REQUIRE(dataSet != nullptr);
  REQUIRE(dataSet->hasNext());
  std::shared_ptr<RowRecord> record = dataSet->next();
  REQUIRE(record != nullptr);
  REQUIRE(record->fields.size() == 2);
  REQUIRE(record->fields[0].longV.value() == 1);
  REQUIRE(record->fields[1].intV.value() == 42);
  REQUIRE_FALSE(dataSet->hasNext());
  dataSet->closeOperationHandle();

  session->executeNonQueryStatement("DROP DATABASE IF EXISTS cpp_ssl_it_table");
  session->close();
}

TEST_CASE("TLS C tree Session connects to IoTDB", "[rpc][ssl][iotdb][e2e]") {
  CSession* session = ts_session_new("127.0.0.1", 6667, "root", "root");
  REQUIRE(session != nullptr);
  it_ssl_configure_tree_session(session);
  REQUIRE(ts_session_open(session) == TS_OK);

  const char* path = "root.cpp_ssl_it_c.d1.s1";
  bool exists = false;
  REQUIRE(ts_session_check_timeseries_exists(session, path, &exists) == TS_OK);
  if (exists) {
    REQUIRE(ts_session_delete_timeseries(session, path) == TS_OK);
  }

  REQUIRE(ts_session_create_database(session, "root.cpp_ssl_it_c") == TS_OK);
  REQUIRE(ts_session_create_timeseries(session, path, TS_TYPE_INT32, TS_ENCODING_PLAIN,
                                       TS_COMPRESSION_UNCOMPRESSED) == TS_OK);
  const char* measurements[] = {"s1"};
  const char* values[] = {"1"};
  REQUIRE(ts_session_insert_record_str(session, "root.cpp_ssl_it_c.d1", 1, 1, measurements, values) ==
          TS_OK);

  CSessionDataSet* dataSet = nullptr;
  REQUIRE(ts_session_execute_query(session, "SELECT s1 FROM root.cpp_ssl_it_c.d1", &dataSet) == TS_OK);
  REQUIRE(dataSet != nullptr);
  REQUIRE(ts_dataset_has_next(dataSet));
  CRowRecord* record = ts_dataset_next(dataSet);
  REQUIRE(record != nullptr);
  REQUIRE(ts_row_record_get_timestamp(record) == 1);
  REQUIRE(ts_row_record_get_field_count(record) == 1);
  REQUIRE(ts_row_record_get_int32(record, 0) == 1);
  ts_row_record_destroy(record);
  REQUIRE_FALSE(ts_dataset_has_next(dataSet));
  ts_dataset_destroy(dataSet);

  REQUIRE(ts_session_delete_timeseries(session, path) == TS_OK);
  REQUIRE(ts_session_delete_database(session, "root.cpp_ssl_it_c") == TS_OK);
  REQUIRE(ts_session_close(session) == TS_OK);
  ts_session_destroy(session);
}

TEST_CASE("TLS C table Session connects to IoTDB", "[rpc][ssl][iotdb][e2e]") {
  CTableSession* session = ts_table_session_new("127.0.0.1", 6667, "root", "root", "");
  REQUIRE(session != nullptr);
  it_ssl_configure_table_session(session);
  REQUIRE(ts_table_session_open(session) == TS_OK);

  REQUIRE(ts_table_session_execute_non_query(session, "CREATE DATABASE IF NOT EXISTS cpp_ssl_it_c_table") ==
          TS_OK);
  REQUIRE(ts_table_session_execute_non_query(session, "USE cpp_ssl_it_c_table") == TS_OK);
  REQUIRE(ts_table_session_execute_non_query(
              session,
              "CREATE TABLE IF NOT EXISTS ssl_it_c_table (tag1 STRING TAG, value INT32 FIELD)") == TS_OK);
  REQUIRE(ts_table_session_execute_non_query(
              session, "INSERT INTO ssl_it_c_table(time, tag1, value) VALUES (1, 't1', 42)") == TS_OK);

  CSessionDataSet* dataSet = nullptr;
  REQUIRE(ts_table_session_execute_query(
              session, "SELECT time, value FROM ssl_it_c_table WHERE tag1 = 't1'", &dataSet) == TS_OK);
  REQUIRE(dataSet != nullptr);
  REQUIRE(ts_dataset_has_next(dataSet));
  CRowRecord* record = ts_dataset_next(dataSet);
  REQUIRE(record != nullptr);
  REQUIRE(ts_row_record_get_field_count(record) >= 2);
  REQUIRE(ts_row_record_get_int64(record, 0) == 1);
  REQUIRE(ts_row_record_get_int32(record, 1) == 42);
  ts_row_record_destroy(record);
  ts_dataset_destroy(dataSet);

  REQUIRE(ts_table_session_execute_non_query(session, "DROP DATABASE IF EXISTS cpp_ssl_it_c_table") == TS_OK);
  REQUIRE(ts_table_session_close(session) == TS_OK);
  ts_table_session_destroy(session);
}

TEST_CASE("Plain client cannot connect to TLS-enabled IoTDB", "[rpc][ssl][iotdb][e2e]") {
  Session session("127.0.0.1", 6667, "root", "root");
  REQUIRE_THROWS_AS(session.open(false), IoTDBException);
}

#endif // WITH_SSL && IOTDB_RPC_SSL_IT
