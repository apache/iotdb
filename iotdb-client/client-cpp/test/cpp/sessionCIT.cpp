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
#include "SessionC.h"
#include <cmath>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

extern CSession* g_session;

static int global_test_id = 0;

class CaseReporter {
public:
  CaseReporter(const char* caseNameArg) : caseName(caseNameArg) {
    test_id = global_test_id++;
    std::cout << "C-API Test " << test_id << ": " << caseName << std::endl;
  }
  ~CaseReporter() {
    std::cout << "C-API Test " << test_id << ": " << caseName << " Done" << std::endl << std::endl;
  }

private:
  const char* caseName;
  int test_id;
};

static const char* testTimeseries[] = {"root.ctest.d1.s1", "root.ctest.d1.s2", "root.ctest.d1.s3"};
static const int testTimeseriesCount = 3;

static void dropTimeseriesIfExists(CSession* session, const char* path) {
  bool exists = false;
  REQUIRE(ts_session_check_timeseries_exists(session, path, &exists) == TS_OK);
  if (exists) {
    REQUIRE(ts_session_delete_timeseries(session, path) == TS_OK);
  }
}

static void ensureTimeseries(CSession* session, const char* path, TSDataType_C type,
                             TSEncoding_C encoding, TSCompressionType_C compression) {
  dropTimeseriesIfExists(session, path);
  REQUIRE(ts_session_create_timeseries(session, path, type, encoding, compression) == TS_OK);
}

static int queryRowCount(CSession* session, const char* sql, int fetchSize = 1024) {
  CSessionDataSet* dataSet = nullptr;
  REQUIRE(ts_session_execute_query(session, sql, &dataSet) == TS_OK);
  REQUIRE(dataSet != nullptr);
  ts_dataset_set_fetch_size(dataSet, fetchSize);
  int count = 0;
  while (ts_dataset_has_next(dataSet)) {
    CRowRecord* record = ts_dataset_next(dataSet);
    REQUIRE(record != nullptr);
    ++count;
    ts_row_record_destroy(record);
  }
  ts_dataset_destroy(dataSet);
  return count;
}

static void dropDatabaseIfExists(CSession* session, const char* database) {
  TsStatus status = ts_session_delete_database(session, database);
  (void)status;
}

static void prepareTimeseries() {
  for (int i = 0; i < testTimeseriesCount; i++) {
    ensureTimeseries(g_session, testTimeseries[i], TS_TYPE_INT64, TS_ENCODING_RLE,
                     TS_COMPRESSION_SNAPPY);
  }
}

/* ============================================================
 *  Timeseries CRUD
 * ============================================================ */

TEST_CASE("C API - Create timeseries", "[c_createTimeseries]") {
  CaseReporter cr("c_createTimeseries");
  const char* path = "root.ctest.d1.s1";
  ensureTimeseries(g_session, path, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
  bool exists = false;
  REQUIRE(ts_session_check_timeseries_exists(g_session, path, &exists) == TS_OK);
  REQUIRE(exists);
  REQUIRE(ts_session_delete_timeseries(g_session, path) == TS_OK);
}

TEST_CASE("C API - Delete timeseries", "[c_deleteTimeseries]") {
  CaseReporter cr("c_deleteTimeseries");
  const char* path = "root.ctest.d1.s1";
  ensureTimeseries(g_session, path, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
  REQUIRE(ts_session_delete_timeseries(g_session, path) == TS_OK);
  bool exists = true;
  REQUIRE(ts_session_check_timeseries_exists(g_session, path, &exists) == TS_OK);
  REQUIRE_FALSE(exists);
}

TEST_CASE("C API - Login failure", "[c_Authentication]") {
  CaseReporter cr("c_LoginTest");
  CSession* badSession = ts_session_new("127.0.0.1", 6667, "root", "wrong-password");
  REQUIRE(badSession != nullptr);
  TsStatus status = ts_session_open(badSession);
  REQUIRE(status != TS_OK);
  const char* err = ts_get_last_error();
  REQUIRE(std::string(err).find("801") != std::string::npos);
  ts_session_destroy(badSession);
}

/* ============================================================
 *  Insert Record (string values)
 * ============================================================ */

TEST_CASE("C API - Insert record by string", "[c_insertRecordStr]") {
  CaseReporter cr("c_insertRecordStr");
  prepareTimeseries();
  const char* deviceId = "root.ctest.d1";
  const char* measurements[] = {"s1", "s2", "s3"};

  for (int64_t time = 0; time < 100; time++) {
    const char* values[] = {"1", "2", "3"};
    TsStatus status =
        ts_session_insert_record_str(g_session, deviceId, time, 3, measurements, values);
    REQUIRE(status == TS_OK);
  }

  CSessionDataSet* dataSet = nullptr;
  TsStatus status =
      ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet);
  REQUIRE(status == TS_OK);
  REQUIRE(dataSet != nullptr);
  ts_dataset_set_fetch_size(dataSet, 1024);
  int count = 0;
  while (ts_dataset_has_next(dataSet)) {
    CRowRecord* record = ts_dataset_next(dataSet);
    REQUIRE(record != nullptr);
    REQUIRE(ts_row_record_get_int64(record, 0) == 1);
    REQUIRE(ts_row_record_get_int64(record, 1) == 2);
    REQUIRE(ts_row_record_get_int64(record, 2) == 3);
    ++count;
    ts_row_record_destroy(record);
  }
  REQUIRE(count == 100);
  ts_dataset_destroy(dataSet);
}

/* ============================================================
 *  Insert Record (typed values)
 * ============================================================ */

TEST_CASE("C API - Insert record with types", "[c_insertRecordTyped]") {
  CaseReporter cr("c_insertRecordTyped");

  const char* timeseries[] = {"root.ctest.d1.s1", "root.ctest.d1.s2", "root.ctest.d1.s3"};
  TSDataType_C types[] = {TS_TYPE_INT32, TS_TYPE_DOUBLE, TS_TYPE_INT64};
  TSEncoding_C encodings[] = {TS_ENCODING_RLE, TS_ENCODING_RLE, TS_ENCODING_RLE};
  TSCompressionType_C compressions[] = {TS_COMPRESSION_SNAPPY, TS_COMPRESSION_SNAPPY,
                                        TS_COMPRESSION_SNAPPY};

  for (int i = 0; i < 3; i++) {
    ensureTimeseries(g_session, timeseries[i], types[i], encodings[i], compressions[i]);
  }

  const char* deviceId = "root.ctest.d1";
  const char* measurements[] = {"s1", "s2", "s3"};

  for (int64_t time = 0; time < 100; time++) {
    int32_t v1 = 1;
    double v2 = 2.2;
    int64_t v3 = 3;
    const void* values[] = {&v1, &v2, &v3};
    TsStatus status =
        ts_session_insert_record(g_session, deviceId, time, 3, measurements, types, values);
    REQUIRE(status == TS_OK);
  }

  REQUIRE(queryRowCount(g_session, "select s1,s2,s3 from root.ctest.d1") == 100);
}

/* ============================================================
 *  Insert Records (batch, string values)
 * ============================================================ */

TEST_CASE("C API - Insert records batch", "[c_insertRecordsBatch]") {
  CaseReporter cr("c_insertRecordsBatch");
  prepareTimeseries();

  const int BATCH = 100;
  const char* deviceId = "root.ctest.d1";
  const char* measurements[] = {"s1", "s2", "s3"};

  const char* deviceIds[BATCH];
  int64_t times[BATCH];
  int measurementCounts[BATCH];
  const char* const* measurementsList[BATCH];
  const char* values[] = {"1", "2", "3"};
  const char* const* valuesList[BATCH];

  for (int i = 0; i < BATCH; i++) {
    deviceIds[i] = deviceId;
    times[i] = i;
    measurementCounts[i] = 3;
    measurementsList[i] = measurements;
    valuesList[i] = values;
  }

  TsStatus status = ts_session_insert_records_str(g_session, BATCH, deviceIds, times,
                                                  measurementCounts, measurementsList, valuesList);
  REQUIRE(status == TS_OK);

  REQUIRE(queryRowCount(g_session, "select s1,s2,s3 from root.ctest.d1") == BATCH);
}

/* ============================================================
 *  Insert Tablet
 * ============================================================ */

TEST_CASE("C API - Insert tablet", "[c_insertTablet]") {
  CaseReporter cr("c_insertTablet");
  prepareTimeseries();

  const char* columnNames[] = {"s1", "s2", "s3"};
  TSDataType_C dataTypes[] = {TS_TYPE_INT64, TS_TYPE_INT64, TS_TYPE_INT64};

  CTablet* tablet = ts_tablet_new("root.ctest.d1", 3, columnNames, dataTypes, 100);
  REQUIRE(tablet != nullptr);

  for (int64_t time = 0; time < 100; time++) {
    ts_tablet_add_timestamp(tablet, (int)time, time);
    for (int col = 0; col < 3; col++) {
      int64_t val = col;
      ts_tablet_add_value_int64(tablet, col, (int)time, val);
    }
  }
  ts_tablet_set_row_count(tablet, 100);

  TsStatus status = ts_session_insert_tablet(g_session, tablet, false);
  REQUIRE(status == TS_OK);

  CSessionDataSet* dataSet = nullptr;
  REQUIRE(ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet) ==
          TS_OK);
  REQUIRE(dataSet != nullptr);
  ts_dataset_set_fetch_size(dataSet, 1024);
  int count = 0;
  while (ts_dataset_has_next(dataSet)) {
    CRowRecord* record = ts_dataset_next(dataSet);
    REQUIRE(ts_row_record_get_int64(record, 0) == 0);
    REQUIRE(ts_row_record_get_int64(record, 1) == 1);
    REQUIRE(ts_row_record_get_int64(record, 2) == 2);
    ++count;
    ts_row_record_destroy(record);
  }
  REQUIRE(count == 100);
  ts_dataset_destroy(dataSet);
  ts_tablet_destroy(tablet);
}

/* ============================================================
 *  Execute SQL directly
 * ============================================================ */

TEST_CASE("C API - Execute non-query SQL", "[c_executeNonQuery]") {
  CaseReporter cr("c_executeNonQuery");
  prepareTimeseries();

  TsStatus status = ts_session_execute_non_query(
      g_session, "insert into root.ctest.d1(timestamp,s1,s2,s3) values(200,10,20,30)");
  REQUIRE(status == TS_OK);

  CSessionDataSet* dataSet = nullptr;
  ts_session_execute_query(g_session, "select s1 from root.ctest.d1 where time=200", &dataSet);
  REQUIRE(ts_dataset_has_next(dataSet));
  CRowRecord* record = ts_dataset_next(dataSet);
  REQUIRE(ts_row_record_get_int64(record, 0) == 10);
  ts_row_record_destroy(record);
  ts_dataset_destroy(dataSet);
}

/* ============================================================
 *  Raw data query
 * ============================================================ */

TEST_CASE("C API - Execute raw data query", "[c_executeRawDataQuery]") {
  CaseReporter cr("c_executeRawDataQuery");
  prepareTimeseries();

  const char* deviceId = "root.ctest.d1";
  const char* measurements[] = {"s1", "s2", "s3"};

  for (int64_t time = 0; time < 50; time++) {
    const char* values[] = {"1", "2", "3"};
    ts_session_insert_record_str(g_session, deviceId, time, 3, measurements, values);
  }

  const char* paths[] = {"root.ctest.d1.s1", "root.ctest.d1.s2", "root.ctest.d1.s3"};
  CSessionDataSet* dataSet = nullptr;
  TsStatus status = ts_session_execute_raw_data_query(g_session, 3, paths, 0, 50, &dataSet);
  REQUIRE(status == TS_OK);

  int count = 0;
  while (ts_dataset_has_next(dataSet)) {
    CRowRecord* record = ts_dataset_next(dataSet);
    count++;
    ts_row_record_destroy(record);
  }
  REQUIRE(count == 50);
  ts_dataset_destroy(dataSet);
}

/* ============================================================
 *  Data deletion
 * ============================================================ */

TEST_CASE("C API - Delete data", "[c_deleteData]") {
  CaseReporter cr("c_deleteData");
  prepareTimeseries();

  const char* deviceId = "root.ctest.d1";
  const char* measurements[] = {"s1", "s2", "s3"};
  for (int64_t time = 0; time < 100; time++) {
    const char* values[] = {"1", "2", "3"};
    ts_session_insert_record_str(g_session, deviceId, time, 3, measurements, values);
  }

  const char* paths[] = {"root.ctest.d1.s1", "root.ctest.d1.s2", "root.ctest.d1.s3"};
  TsStatus status = ts_session_delete_data_batch(g_session, 3, paths, 49);
  REQUIRE(status == TS_OK);

  REQUIRE(queryRowCount(g_session, "select s1,s2,s3 from root.ctest.d1") == 50);
}

/* ============================================================
 *  Timezone
 * ============================================================ */

TEST_CASE("C API - Timezone", "[c_timezone]") {
  CaseReporter cr("c_timezone");
  char buf[64] = {0};
  TsStatus status = ts_session_get_timezone(g_session, buf, sizeof(buf));
  REQUIRE(status == TS_OK);
  REQUIRE(strlen(buf) > 0);

  status = ts_session_set_timezone(g_session, "Asia/Shanghai");
  REQUIRE(status == TS_OK);

  memset(buf, 0, sizeof(buf));
  ts_session_get_timezone(g_session, buf, sizeof(buf));
  REQUIRE(std::string(buf) == "Asia/Shanghai");
}

/* ============================================================
 *  Multi-node constructor
 * ============================================================ */

TEST_CASE("C API - Multi-node session", "[c_multiNode]") {
  CaseReporter cr("c_multiNode");
  const char* urls[] = {"127.0.0.1:6667"};
  CSession* localSession = ts_session_new_multi_node(urls, 1, "root", "root");
  REQUIRE(localSession != nullptr);

  TsStatus status = ts_session_open(localSession);
  REQUIRE(status == TS_OK);

  const char* path = "root.ctest.d1.s1";
  ensureTimeseries(localSession, path, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
  bool exists = false;
  REQUIRE(ts_session_check_timeseries_exists(localSession, path, &exists) == TS_OK);
  REQUIRE(exists);
  REQUIRE(ts_session_delete_timeseries(localSession, path) == TS_OK);

  ts_session_close(localSession);
  ts_session_destroy(localSession);
}

/* ============================================================
 *  Dataset column info
 * ============================================================ */

TEST_CASE("C API - Dataset column info", "[c_datasetColumns]") {
  CaseReporter cr("c_datasetColumns");
  prepareTimeseries();

  const char* deviceId = "root.ctest.d1";
  const char* measurements[] = {"s1", "s2", "s3"};
  const char* values[] = {"1", "2", "3"};
  ts_session_insert_record_str(g_session, deviceId, 0, 3, measurements, values);

  CSessionDataSet* dataSet = nullptr;
  ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet);
  REQUIRE(dataSet != nullptr);

  int colCount = ts_dataset_get_column_count(dataSet);
  REQUIRE(colCount == 4); // Time + s1 + s2 + s3

  const char* col0 = ts_dataset_get_column_name(dataSet, 0);
  REQUIRE(std::string(col0) == "Time");

  int n = ts_dataset_get_column_count(dataSet);
  for (int i = 0; i < n; i++) {
    const char* ct = ts_dataset_get_column_type(dataSet, i);
    REQUIRE(ct != nullptr);
    REQUIRE(strlen(ct) > 0);
  }

  ts_dataset_destroy(dataSet);
}

/* ============================================================
 *  SessionC.h API coverage (tree model) — additional smoke tests
 * ============================================================ */

TEST_CASE("C API - Session lifecycle variants", "[c_sessionLifecycle]") {
  CaseReporter cr("c_sessionLifecycle");

  CSession* s1 = ts_session_new_with_zone("127.0.0.1", 6667, "root", "root", "Asia/Shanghai", 1024);
  REQUIRE(s1 != nullptr);
  REQUIRE(ts_session_open(s1) == TS_OK);
  ts_session_close(s1);
  ts_session_destroy(s1);

  CSession* s2 = ts_session_new("127.0.0.1", 6667, "root", "root");
  REQUIRE(s2 != nullptr);
  REQUIRE(ts_session_open_with_compression(s2, true) == TS_OK);
  ts_session_close(s2);
  ts_session_destroy(s2);
}

TEST_CASE("C API - Database and extended timeseries APIs", "[c_dbTimeseries]") {
  CaseReporter cr("c_dbTimeseries");

  const char* sg1 = "root.cov_sg_a";
  const char* sg2 = "root.cov_sg_b";
  dropDatabaseIfExists(g_session, sg1);
  dropDatabaseIfExists(g_session, sg2);
  REQUIRE(ts_session_create_database(g_session, sg1) == TS_OK);
  REQUIRE(ts_session_create_database(g_session, sg2) == TS_OK);
  const char* dbs[] = {sg1, sg2};
  REQUIRE(ts_session_delete_databases(g_session, dbs, 2) == TS_OK);

  const char* sg3 = "root.cov_sg_c";
  dropDatabaseIfExists(g_session, sg3);
  REQUIRE(ts_session_create_database(g_session, sg3) == TS_OK);
  REQUIRE(ts_session_delete_database(g_session, sg3) == TS_OK);

  const char* sgEx = "root.cov_sg_ex";
  dropDatabaseIfExists(g_session, sgEx);
  REQUIRE(ts_session_create_database(g_session, sgEx) == TS_OK);

  const char* pathEx = "root.cov_sg_ex.d1.s_ex";
  dropTimeseriesIfExists(g_session, pathEx);
  REQUIRE(ts_session_create_timeseries_ex(g_session, pathEx, TS_TYPE_INT64, TS_ENCODING_RLE,
                                          TS_COMPRESSION_SNAPPY, 0, nullptr, nullptr, 0, nullptr,
                                          nullptr, 0, nullptr, nullptr, nullptr) == TS_OK);

  const char* pathsM[] = {"root.cov_sg_ex.d1.s_m1", "root.cov_sg_ex.d1.s_m2"};
  TSDataType_C tsM[] = {TS_TYPE_INT64, TS_TYPE_DOUBLE};
  TSEncoding_C encM[] = {TS_ENCODING_RLE, TS_ENCODING_RLE};
  TSCompressionType_C compM[] = {TS_COMPRESSION_SNAPPY, TS_COMPRESSION_SNAPPY};
  for (int i = 0; i < 2; i++) {
    dropTimeseriesIfExists(g_session, pathsM[i]);
  }
  REQUIRE(ts_session_create_multi_timeseries(g_session, 2, pathsM, tsM, encM, compM) == TS_OK);
  REQUIRE(ts_session_delete_timeseries_batch(g_session, pathsM, 2) == TS_OK);
  REQUIRE(ts_session_delete_timeseries(g_session, pathEx) == TS_OK);
  REQUIRE(ts_session_delete_database(g_session, sgEx) == TS_OK);
}

TEST_CASE("C API - Tablet row count and reset", "[c_tabletReset]") {
  CaseReporter cr("c_tabletReset");
  const char* colNames[] = {"s1"};
  TSDataType_C dts[] = {TS_TYPE_INT64};
  CTablet* tablet = ts_tablet_new("root.ctest.d1", 1, colNames, dts, 10);
  REQUIRE(tablet != nullptr);
  REQUIRE(ts_tablet_get_row_count(tablet) == 0);
  REQUIRE(ts_tablet_set_row_count(tablet, 1) == TS_OK);
  REQUIRE(ts_tablet_get_row_count(tablet) == 1);
  ts_tablet_reset(tablet);
  REQUIRE(ts_tablet_get_row_count(tablet) == 0);
  ts_tablet_destroy(tablet);
}

TEST_CASE("C API - Aligned timeseries and aligned writes", "[c_aligned]") {
  CaseReporter cr("c_aligned");

  const char* sg = "root.cov_al";
  dropDatabaseIfExists(g_session, sg);
  REQUIRE(ts_session_create_database(g_session, sg) == TS_OK);

  const char* alDev = "root.cov_al.dev";
  const char* meas[] = {"m1", "m2"};
  TSDataType_C adt[] = {TS_TYPE_INT64, TS_TYPE_INT64};
  TSEncoding_C aenc[] = {TS_ENCODING_RLE, TS_ENCODING_RLE};
  TSCompressionType_C acomp[] = {TS_COMPRESSION_SNAPPY, TS_COMPRESSION_SNAPPY};
  REQUIRE(ts_session_create_aligned_timeseries(g_session, alDev, 2, meas, adt, aenc, acomp) ==
          TS_OK);

  const char* mstr[] = {"m1", "m2"};
  const char* vstr[] = {"1", "2"};
  REQUIRE(ts_session_insert_aligned_record_str(g_session, alDev, 100LL, 2, mstr, vstr) == TS_OK);

  int64_t v1 = 3;
  int64_t v2 = 4;
  const void* vals[] = {&v1, &v2};
  REQUIRE(ts_session_insert_aligned_record(g_session, alDev, 101LL, 2, mstr, adt, vals) == TS_OK);

  const char* devs1[] = {alDev};
  int64_t times1[] = {102LL};
  int mc1[] = {2};
  const char* const* mlist1[] = {mstr};
  const char* const* vlist1[] = {vstr};
  REQUIRE(ts_session_insert_aligned_records_str(g_session, 1, devs1, times1, mc1, mlist1, vlist1) ==
          TS_OK);

  const TSDataType_C* trows[] = {adt};
  const void* const* vrows[] = {vals};
  REQUIRE(ts_session_insert_aligned_records(g_session, 1, devs1, times1, mc1, mlist1, trows,
                                            vrows) == TS_OK);

  int64_t tRows[] = {104LL, 105LL};
  int mcRows[] = {2, 2};
  const char* const* mRows[] = {mstr, mstr};
  const TSDataType_C* tRowsList[] = {adt, adt};
  int64_t v1a = 5, v1b = 6;
  int64_t v2a = 7, v2b = 8;
  const void* row0[] = {&v1a, &v2a};
  const void* row1[] = {&v1b, &v2b};
  const void* const* vRowsList[] = {row0, row1};
  REQUIRE(ts_session_insert_aligned_records_of_one_device(g_session, alDev, 2, tRows, mcRows, mRows,
                                                          tRowsList, vRowsList, true) == TS_OK);

  const char* alDev2 = "root.cov_al.dev2";
  REQUIRE(ts_session_create_aligned_timeseries(g_session, alDev2, 2, meas, adt, aenc, acomp) ==
          TS_OK);
  CTablet* tab = ts_tablet_new(alDev, 2, meas, adt, 10);
  CTablet* tab2 = ts_tablet_new(alDev2, 2, meas, adt, 5);
  REQUIRE(tab != nullptr);
  REQUIRE(tab2 != nullptr);
  ts_tablet_add_timestamp(tab, 0, 106LL);
  ts_tablet_add_value_int64(tab, 0, 0, 9);
  ts_tablet_add_value_int64(tab, 1, 0, 10);
  ts_tablet_set_row_count(tab, 1);
  ts_tablet_add_timestamp(tab2, 0, 107LL);
  ts_tablet_add_value_int64(tab2, 0, 0, 11);
  ts_tablet_add_value_int64(tab2, 1, 0, 12);
  ts_tablet_set_row_count(tab2, 1);
  const char* devIds[] = {alDev, alDev2};
  CTablet* tabs[] = {tab, tab2};
  REQUIRE(ts_session_insert_aligned_tablets(g_session, 2, devIds, tabs, false) == TS_OK);

  ts_tablet_reset(tab);
  ts_tablet_add_timestamp(tab, 0, 200LL);
  ts_tablet_add_value_int64(tab, 0, 0, 13);
  ts_tablet_add_value_int64(tab, 1, 0, 14);
  ts_tablet_set_row_count(tab, 1);
  REQUIRE(ts_session_insert_aligned_tablet(g_session, tab, false) == TS_OK);

  ts_tablet_destroy(tab2);
  ts_tablet_destroy(tab);

  REQUIRE(ts_session_delete_database(g_session, sg) == TS_OK);
}

TEST_CASE("C API - Typed batch inserts and insert_tablets", "[c_batchTablet]") {
  CaseReporter cr("c_batchTablet");

  const char* sg = "root.cov_batch";
  dropDatabaseIfExists(g_session, sg);
  REQUIRE(ts_session_create_database(g_session, sg) == TS_OK);

  const char* p1 = "root.cov_batch.da.s1";
  const char* p2 = "root.cov_batch.db.s1";
  ensureTimeseries(g_session, p1, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
  ensureTimeseries(g_session, p2, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);

  const char* devIds[] = {"root.cov_batch.da", "root.cov_batch.db"};
  int64_t tt[] = {1LL, 2LL};
  int mmc[] = {1, 1};
  const char* mda[] = {"s1"};
  const char* mdb[] = {"s1"};
  const char* const* mlist[] = {mda, mdb};
  int64_t va = 11;
  int64_t vb = 22;
  const void* vva[] = {&va};
  const void* vvb[] = {&vb};
  const void* const* vlist[] = {vva, vvb};
  TSDataType_C ta[] = {TS_TYPE_INT64};
  TSDataType_C tb[] = {TS_TYPE_INT64};
  const TSDataType_C* tlist[] = {ta, tb};
  REQUIRE(ts_session_insert_records(g_session, 2, devIds, tt, mmc, mlist, tlist, vlist) == TS_OK);

  const char* dc = "root.cov_batch.dc";
  const char* p3 = "root.cov_batch.dc.s1";
  ensureTimeseries(g_session, p3, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
  int64_t tdc[] = {3LL, 4LL};
  int mcdc[] = {1, 1};
  const char* const* mdcList[] = {mda, mda};
  const TSDataType_C* tdcList[] = {ta, ta};
  int64_t vc = 30, vd = 40;
  const void* rv0[] = {&vc};
  const void* rv1[] = {&vd};
  const void* const* vdcList[] = {rv0, rv1};
  REQUIRE(ts_session_insert_records_of_one_device(g_session, dc, 2, tdc, mcdc, mdcList, tdcList,
                                                  vdcList, true) == TS_OK);

  const char* col1[] = {"s1"};
  TSDataType_C dt1[] = {TS_TYPE_INT64};
  CTablet* tb1 = ts_tablet_new("root.cov_batch.ta", 1, col1, dt1, 5);
  CTablet* tb2 = ts_tablet_new("root.cov_batch.tb", 1, col1, dt1, 5);
  REQUIRE(tb1 != nullptr);
  REQUIRE(tb2 != nullptr);
  const char* pta = "root.cov_batch.ta.s1";
  const char* ptb = "root.cov_batch.tb.s1";
  ensureTimeseries(g_session, pta, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
  ensureTimeseries(g_session, ptb, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
  ts_tablet_add_timestamp(tb1, 0, 1LL);
  ts_tablet_add_value_int64(tb1, 0, 0, 100);
  ts_tablet_set_row_count(tb1, 1);
  ts_tablet_add_timestamp(tb2, 0, 2LL);
  ts_tablet_add_value_int64(tb2, 0, 0, 200);
  ts_tablet_set_row_count(tb2, 1);
  const char* tabDevs[] = {"root.cov_batch.ta", "root.cov_batch.tb"};
  CTablet* tbs[] = {tb1, tb2};
  REQUIRE(ts_session_insert_tablets(g_session, 2, tabDevs, tbs, false) == TS_OK);
  ts_tablet_destroy(tb2);
  ts_tablet_destroy(tb1);

  REQUIRE(ts_session_delete_database(g_session, sg) == TS_OK);
}

TEST_CASE("C API - Query timeout and last data queries", "[c_queryLast]") {
  CaseReporter cr("c_queryLast");
  prepareTimeseries();

  const char* deviceId = "root.ctest.d1";
  const char* measurements[] = {"s1", "s2", "s3"};
  for (int64_t time = 300; time < 310; time++) {
    const char* values[] = {"7", "8", "9"};
    REQUIRE(ts_session_insert_record_str(g_session, deviceId, time, 3, measurements, values) ==
            TS_OK);
  }

  const char* paths[] = {"root.ctest.d1.s1", "root.ctest.d1.s2"};
  CSessionDataSet* ds = nullptr;
  REQUIRE(ts_session_execute_query_with_timeout(
              g_session, "select s1 from root.ctest.d1 where time>=300", 60000, &ds) == TS_OK);
  REQUIRE(ds != nullptr);
  ts_dataset_destroy(ds);

  ds = nullptr;
  REQUIRE(ts_session_execute_last_data_query(g_session, 2, paths, &ds) == TS_OK);
  REQUIRE(ds != nullptr);
  ts_dataset_destroy(ds);

  ds = nullptr;
  REQUIRE(ts_session_execute_last_data_query_with_time(g_session, 2, paths, 305LL, &ds) == TS_OK);
  REQUIRE(ds != nullptr);
  ts_dataset_destroy(ds);
}

TEST_CASE("C API - RowRecord and delete data APIs", "[c_rowDelete]") {
  CaseReporter cr("c_rowDelete");

  const char* sg = "root.cov_types";
  dropDatabaseIfExists(g_session, sg);
  REQUIRE(ts_session_create_database(g_session, sg) == TS_OK);

  const char* pb = "root.cov_types.d1.sb";
  const char* pi = "root.cov_types.d1.si";
  const char* pf = "root.cov_types.d1.sf";
  const char* pd = "root.cov_types.d1.sd";
  const char* pt = "root.cov_types.d1.st";
  const char* tpaths[] = {pb, pi, pf, pd, pt};
  for (const char* tp : tpaths) {
    dropTimeseriesIfExists(g_session, tp);
  }
  REQUIRE(ts_session_create_timeseries(g_session, pb, TS_TYPE_BOOLEAN, TS_ENCODING_RLE,
                                       TS_COMPRESSION_SNAPPY) == TS_OK);
  REQUIRE(ts_session_create_timeseries(g_session, pi, TS_TYPE_INT32, TS_ENCODING_RLE,
                                       TS_COMPRESSION_SNAPPY) == TS_OK);
  REQUIRE(ts_session_create_timeseries(g_session, pf, TS_TYPE_FLOAT, TS_ENCODING_RLE,
                                       TS_COMPRESSION_SNAPPY) == TS_OK);
  REQUIRE(ts_session_create_timeseries(g_session, pd, TS_TYPE_DOUBLE, TS_ENCODING_RLE,
                                       TS_COMPRESSION_SNAPPY) == TS_OK);
  REQUIRE(ts_session_create_timeseries(g_session, pt, TS_TYPE_TEXT, TS_ENCODING_PLAIN,
                                       TS_COMPRESSION_SNAPPY) == TS_OK);

  const char* dev = "root.cov_types.d1";
  const char* names[] = {"sb", "si", "sf", "sd", "st"};
  TSDataType_C types[] = {TS_TYPE_BOOLEAN, TS_TYPE_INT32, TS_TYPE_FLOAT, TS_TYPE_DOUBLE,
                          TS_TYPE_TEXT};
  bool bv = true;
  int32_t iv = 42;
  float fv = 2.5f;
  double dv = 3.25;
  const char* tv = "hi";
  const void* vals[] = {&bv, &iv, &fv, &dv, tv};
  REQUIRE(ts_session_insert_record(g_session, dev, 500LL, 5, names, types, vals) == TS_OK);

  CSessionDataSet* dataSet = nullptr;
  REQUIRE(ts_session_execute_query(g_session,
                                   "select sb,si,sf,sd,st from root.cov_types.d1 where time=500",
                                   &dataSet) == TS_OK);
  REQUIRE(dataSet != nullptr);
  REQUIRE(ts_dataset_has_next(dataSet));
  CRowRecord* record = ts_dataset_next(dataSet);
  REQUIRE(record != nullptr);
  REQUIRE(ts_row_record_get_timestamp(record) == 500LL);
  REQUIRE(ts_row_record_get_field_count(record) == 5);
  REQUIRE_FALSE(ts_row_record_is_null(record, 0));
  REQUIRE(ts_row_record_get_bool(record, 0) == true);
  REQUIRE(ts_row_record_get_int32(record, 1) == 42);
  REQUIRE(std::fabs(ts_row_record_get_float(record, 2) - 2.5f) < 1e-4f);
  REQUIRE(std::fabs(ts_row_record_get_double(record, 3) - 3.25) < 1e-9);
  REQUIRE(std::string(ts_row_record_get_string(record, 4)) == "hi");
  REQUIRE(ts_row_record_get_data_type(record, 0) == TS_TYPE_BOOLEAN);
  ts_row_record_destroy(record);
  ts_dataset_destroy(dataSet);

  REQUIRE(ts_session_delete_data(g_session, pb, 500LL) == TS_OK);
  const char* delPaths[] = {pi, pf};
  REQUIRE(ts_session_delete_data_range(g_session, 2, delPaths, 400LL, 600LL) == TS_OK);

  REQUIRE(ts_session_delete_timeseries(g_session, pb) == TS_OK);
  REQUIRE(ts_session_delete_timeseries(g_session, pi) == TS_OK);
  REQUIRE(ts_session_delete_timeseries(g_session, pf) == TS_OK);
  REQUIRE(ts_session_delete_timeseries(g_session, pd) == TS_OK);
  REQUIRE(ts_session_delete_timeseries(g_session, pt) == TS_OK);
  REQUIRE(ts_session_delete_database(g_session, sg) == TS_OK);
}
