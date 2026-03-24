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

static void prepareTimeseries() {
    for (int i = 0; i < testTimeseriesCount; i++) {
        bool exists = false;
        ts_session_check_timeseries_exists(g_session, testTimeseries[i], &exists);
        if (exists) {
            ts_session_delete_timeseries(g_session, testTimeseries[i]);
        }
        ts_session_create_timeseries(g_session, testTimeseries[i],
                                     TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
    }
}

/* ============================================================
 *  Timeseries CRUD
 * ============================================================ */

TEST_CASE("C API - Create timeseries", "[c_createTimeseries]") {
    CaseReporter cr("c_createTimeseries");
    const char* path = "root.ctest.d1.s1";
    bool exists = false;
    ts_session_check_timeseries_exists(g_session, path, &exists);
    if (!exists) {
        TsStatus status = ts_session_create_timeseries(g_session, path,
                                                        TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
        REQUIRE(status == TS_OK);
    }
    ts_session_check_timeseries_exists(g_session, path, &exists);
    REQUIRE(exists == true);
    ts_session_delete_timeseries(g_session, path);
}

TEST_CASE("C API - Delete timeseries", "[c_deleteTimeseries]") {
    CaseReporter cr("c_deleteTimeseries");
    const char* path = "root.ctest.d1.s1";

    bool exists = false;
    ts_session_check_timeseries_exists(g_session, path, &exists);
    if (!exists) {
        ts_session_create_timeseries(g_session, path, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
    }
    ts_session_check_timeseries_exists(g_session, path, &exists);
    REQUIRE(exists == true);

    TsStatus status = ts_session_delete_timeseries(g_session, path);
    REQUIRE(status == TS_OK);
    ts_session_check_timeseries_exists(g_session, path, &exists);
    REQUIRE(exists == false);
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
        TsStatus status = ts_session_insert_record_str(g_session, deviceId, time, 3, measurements, values);
        REQUIRE(status == TS_OK);
    }

    CSessionDataSet* dataSet = nullptr;
    TsStatus status = ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet);
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
        count++;
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
    TSCompressionType_C compressions[] = {TS_COMPRESSION_SNAPPY, TS_COMPRESSION_SNAPPY, TS_COMPRESSION_SNAPPY};

    for (int i = 0; i < 3; i++) {
        bool exists = false;
        ts_session_check_timeseries_exists(g_session, timeseries[i], &exists);
        if (exists) ts_session_delete_timeseries(g_session, timeseries[i]);
        ts_session_create_timeseries(g_session, timeseries[i], types[i], encodings[i], compressions[i]);
    }

    const char* deviceId = "root.ctest.d1";
    const char* measurements[] = {"s1", "s2", "s3"};

    for (int64_t time = 0; time < 100; time++) {
        int32_t v1 = 1;
        double v2 = 2.2;
        int64_t v3 = 3;
        const void* values[] = {&v1, &v2, &v3};
        TsStatus status = ts_session_insert_record(g_session, deviceId, time, 3, measurements, types, values);
        REQUIRE(status == TS_OK);
    }

    CSessionDataSet* dataSet = nullptr;
    ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet);
    ts_dataset_set_fetch_size(dataSet, 1024);
    int count = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        count++;
        ts_row_record_destroy(record);
    }
    REQUIRE(count == 100);
    ts_dataset_destroy(dataSet);
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

    CSessionDataSet* dataSet = nullptr;
    ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet);
    ts_dataset_set_fetch_size(dataSet, 1024);
    int count = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        count++;
        ts_row_record_destroy(record);
    }
    REQUIRE(count == BATCH);
    ts_dataset_destroy(dataSet);
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
    ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet);
    ts_dataset_set_fetch_size(dataSet, 1024);
    int count = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        REQUIRE(ts_row_record_get_int64(record, 0) == 0);
        REQUIRE(ts_row_record_get_int64(record, 1) == 1);
        REQUIRE(ts_row_record_get_int64(record, 2) == 2);
        count++;
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

    TsStatus status = ts_session_execute_non_query(g_session,
        "insert into root.ctest.d1(timestamp,s1,s2,s3) values(200,10,20,30)");
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

    CSessionDataSet* dataSet = nullptr;
    ts_session_execute_query(g_session, "select s1,s2,s3 from root.ctest.d1", &dataSet);
    ts_dataset_set_fetch_size(dataSet, 1024);
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
    bool exists = false;
    ts_session_check_timeseries_exists(localSession, path, &exists);
    if (!exists) {
        ts_session_create_timeseries(localSession, path, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY);
    }
    ts_session_check_timeseries_exists(localSession, path, &exists);
    REQUIRE(exists == true);
    ts_session_delete_timeseries(localSession, path);

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
    REQUIRE(colCount == 4);  // Time + s1 + s2 + s3

    const char* col0 = ts_dataset_get_column_name(dataSet, 0);
    REQUIRE(std::string(col0) == "Time");

    ts_dataset_destroy(dataSet);
}
