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
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <cmath>

extern CTableSession* g_table_session;

static int global_test_tag = 0;

class CaseReporter {
public:
    CaseReporter(const char* caseNameArg) : caseName(caseNameArg) {
        test_tag = global_test_tag++;
        std::cout << "C-API Table Test " << test_tag << ": " << caseName << std::endl;
    }
    ~CaseReporter() {
        std::cout << "C-API Table Test " << test_tag << ": " << caseName << " Done" << std::endl << std::endl;
    }
private:
    const char* caseName;
    int test_tag;
};

/* ============================================================
 *  DDL via SQL — create database & table
 * ============================================================ */

TEST_CASE("C API Table - Create table", "[c_table_createTable][c_table_ddl]") {
    CaseReporter cr("c_table_createTable");

    ts_table_session_execute_non_query(g_table_session, "DROP DATABASE IF EXISTS c_db1");
    TsStatus status = ts_table_session_execute_non_query(g_table_session, "CREATE DATABASE c_db1");
    REQUIRE(status == TS_OK);

    ts_table_session_execute_non_query(g_table_session, "USE \"c_db1\"");
    status = ts_table_session_execute_non_query(g_table_session,
        "CREATE TABLE c_table0 ("
        "tag1 string tag,"
        "attr1 string attribute,"
        "m1 double field)");
    REQUIRE(status == TS_OK);

    CSessionDataSet* dataSet = nullptr;
    status = ts_table_session_execute_query(g_table_session, "SHOW TABLES", &dataSet);
    REQUIRE(status == TS_OK);
    REQUIRE(dataSet != nullptr);

    ts_dataset_set_fetch_size(dataSet, 1024);
    bool tableExist = false;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        const char* tableName = ts_row_record_get_string(record, 0);
        if (std::string(tableName) == "c_table0") {
            tableExist = true;
        }
        ts_row_record_destroy(record);
        if (tableExist) break;
    }
    REQUIRE(tableExist == true);
    ts_dataset_destroy(dataSet);
}

/* ============================================================
 *  Insert Tablet (table model, with TAG/FIELD/ATTRIBUTE columns)
 * ============================================================ */

TEST_CASE("C API Table - Insert tablet", "[c_table_insertTablet][c_table_write]") {
    CaseReporter cr("c_table_insertTablet");

    ts_table_session_execute_non_query(g_table_session, "DROP DATABASE IF EXISTS c_db2");
    ts_table_session_execute_non_query(g_table_session, "CREATE DATABASE c_db2");
    ts_table_session_execute_non_query(g_table_session, "USE \"c_db2\"");
    ts_table_session_execute_non_query(g_table_session,
        "CREATE TABLE c_table1 ("
        "tag1 string tag,"
        "attr1 string attribute,"
        "m1 double field)");

    const char* columnNames[] = {"tag1", "attr1", "m1"};
    TSDataType_C dataTypes[] = {TS_TYPE_STRING, TS_TYPE_STRING, TS_TYPE_DOUBLE};
    TSColumnCategory_C colCategories[] = {TS_COL_TAG, TS_COL_ATTRIBUTE, TS_COL_FIELD};

    CTablet* tablet = ts_tablet_new_with_category("c_table1", 3, columnNames, dataTypes,
                                                    colCategories, 100);
    REQUIRE(tablet != nullptr);

    for (int i = 0; i < 50; i++) {
        ts_tablet_add_timestamp(tablet, i, (int64_t)i);
        ts_tablet_add_value_string(tablet, 0, i, "device_A");
        ts_tablet_add_value_string(tablet, 1, i, "attr_val");
        ts_tablet_add_value_double(tablet, 2, i, i * 1.5);
    }
    ts_tablet_set_row_count(tablet, 50);

    TsStatus status = ts_table_session_insert(g_table_session, tablet);
    REQUIRE(status == TS_OK);

    CSessionDataSet* dataSet = nullptr;
    status = ts_table_session_execute_query(g_table_session, "SELECT * FROM c_table1", &dataSet);
    REQUIRE(status == TS_OK);

    ts_dataset_set_fetch_size(dataSet, 1024);
    int count = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        count++;
        ts_row_record_destroy(record);
    }
    REQUIRE(count == 50);
    ts_dataset_destroy(dataSet);
    ts_tablet_destroy(tablet);
}

/* ============================================================
 *  Query with timeout
 * ============================================================ */

TEST_CASE("C API Table - Query with timeout", "[c_table_queryTimeout][c_table_query]") {
    CaseReporter cr("c_table_queryTimeout");

    ts_table_session_execute_non_query(g_table_session, "DROP DATABASE IF EXISTS c_db3");
    ts_table_session_execute_non_query(g_table_session, "CREATE DATABASE c_db3");
    ts_table_session_execute_non_query(g_table_session, "USE \"c_db3\"");
    ts_table_session_execute_non_query(g_table_session,
        "CREATE TABLE c_table2 (tag1 string tag, m1 int32 field)");

    const char* columnNames[] = {"tag1", "m1"};
    TSDataType_C dataTypes[] = {TS_TYPE_STRING, TS_TYPE_INT32};
    TSColumnCategory_C colCategories[] = {TS_COL_TAG, TS_COL_FIELD};

    CTablet* tablet = ts_tablet_new_with_category("c_table2", 2, columnNames, dataTypes,
                                                    colCategories, 10);
    for (int i = 0; i < 10; i++) {
        ts_tablet_add_timestamp(tablet, i, (int64_t)i);
        ts_tablet_add_value_string(tablet, 0, i, "dev1");
        ts_tablet_add_value_int32(tablet, 1, i, i * 10);
    }
    ts_tablet_set_row_count(tablet, 10);
    ts_table_session_insert(g_table_session, tablet);
    ts_tablet_destroy(tablet);

    CSessionDataSet* dataSet = nullptr;
    TsStatus status = ts_table_session_execute_query_with_timeout(g_table_session,
        "SELECT * FROM c_table2", 60000, &dataSet);
    REQUIRE(status == TS_OK);

    int count = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        count++;
        ts_row_record_destroy(record);
    }
    REQUIRE(count == 10);
    ts_dataset_destroy(dataSet);
}

/* ============================================================
 *  Multi-type tablet insert
 * ============================================================ */

TEST_CASE("C API Table - Multi-type tablet", "[c_table_multiType][c_table_write]") {
    CaseReporter cr("c_table_multiType");

    ts_table_session_execute_non_query(g_table_session, "DROP DATABASE IF EXISTS c_db4");
    ts_table_session_execute_non_query(g_table_session, "CREATE DATABASE c_db4");
    ts_table_session_execute_non_query(g_table_session, "USE \"c_db4\"");
    ts_table_session_execute_non_query(g_table_session,
        "CREATE TABLE c_table3 ("
        "tag1 string tag,"
        "m_bool boolean field,"
        "m_int32 int32 field,"
        "m_int64 int64 field,"
        "m_float float field,"
        "m_double double field,"
        "m_text text field)");

    const char* columnNames[] = {"tag1", "m_bool", "m_int32", "m_int64", "m_float", "m_double", "m_text"};
    TSDataType_C dataTypes[] = {TS_TYPE_STRING, TS_TYPE_BOOLEAN, TS_TYPE_INT32, TS_TYPE_INT64,
                                TS_TYPE_FLOAT, TS_TYPE_DOUBLE, TS_TYPE_TEXT};
    TSColumnCategory_C colCategories[] = {TS_COL_TAG, TS_COL_FIELD, TS_COL_FIELD, TS_COL_FIELD,
                                          TS_COL_FIELD, TS_COL_FIELD, TS_COL_FIELD};

    CTablet* tablet = ts_tablet_new_with_category("c_table3", 7, columnNames, dataTypes,
                                                    colCategories, 20);
    for (int i = 0; i < 20; i++) {
        ts_tablet_add_timestamp(tablet, i, (int64_t)(i + 1000));
        ts_tablet_add_value_string(tablet, 0, i, "dev1");
        ts_tablet_add_value_bool(tablet, 1, i, (i % 2 == 0));
        ts_tablet_add_value_int32(tablet, 2, i, i * 10);
        ts_tablet_add_value_int64(tablet, 3, i, (int64_t)i * 100);
        ts_tablet_add_value_float(tablet, 4, i, i * 1.1f);
        ts_tablet_add_value_double(tablet, 5, i, i * 2.2);
        ts_tablet_add_value_string(tablet, 6, i, "hello");
    }
    ts_tablet_set_row_count(tablet, 20);

    TsStatus status = ts_table_session_insert(g_table_session, tablet);
    REQUIRE(status == TS_OK);

    CSessionDataSet* dataSet = nullptr;
    status = ts_table_session_execute_query(g_table_session, "SELECT * FROM c_table3", &dataSet);
    REQUIRE(status == TS_OK);

    int count = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        count++;
        ts_row_record_destroy(record);
    }
    REQUIRE(count == 20);
    ts_dataset_destroy(dataSet);
    ts_tablet_destroy(tablet);
}

/* ============================================================
 *  Multi-node table session
 * ============================================================ */

TEST_CASE("C API Table - Multi-node table session", "[c_table_multiNode][c_table_lifecycle]") {
    CaseReporter cr("c_table_multiNode");

    const char* urls[] = {"127.0.0.1:6667"};
    CTableSession* localSession = ts_table_session_new_multi_node(urls, 1, "root", "root", "");
    REQUIRE(localSession != nullptr);

    TsStatus status = ts_table_session_execute_non_query(localSession, "DROP DATABASE IF EXISTS c_db5");
    REQUIRE(status == TS_OK);
    ts_table_session_execute_non_query(localSession, "CREATE DATABASE c_db5");

    ts_table_session_close(localSession);
    ts_table_session_destroy(localSession);
}

/* ============================================================
 *  Dataset column info (table model)
 * ============================================================ */

TEST_CASE("C API Table - Dataset column info", "[c_table_datasetColumns][c_table_query]") {
    CaseReporter cr("c_table_datasetColumns");

    ts_table_session_execute_non_query(g_table_session, "DROP DATABASE IF EXISTS c_db6");
    ts_table_session_execute_non_query(g_table_session, "CREATE DATABASE c_db6");
    ts_table_session_execute_non_query(g_table_session, "USE \"c_db6\"");
    ts_table_session_execute_non_query(g_table_session,
        "CREATE TABLE c_table6 (tag1 string tag, m1 int64 field)");

    const char* columnNames[] = {"tag1", "m1"};
    TSDataType_C dataTypes[] = {TS_TYPE_STRING, TS_TYPE_INT64};
    TSColumnCategory_C colCategories[] = {TS_COL_TAG, TS_COL_FIELD};

    CTablet* tablet = ts_tablet_new_with_category("c_table6", 2, columnNames, dataTypes,
                                                    colCategories, 5);
    for (int i = 0; i < 5; i++) {
        ts_tablet_add_timestamp(tablet, i, (int64_t)i);
        ts_tablet_add_value_string(tablet, 0, i, "dev1");
        ts_tablet_add_value_int64(tablet, 1, i, (int64_t)(i * 100));
    }
    ts_tablet_set_row_count(tablet, 5);
    ts_table_session_insert(g_table_session, tablet);
    ts_tablet_destroy(tablet);

    CSessionDataSet* dataSet = nullptr;
    ts_table_session_execute_query(g_table_session, "SELECT * FROM c_table6", &dataSet);
    REQUIRE(dataSet != nullptr);

    int colCount = ts_dataset_get_column_count(dataSet);
    REQUIRE(colCount >= 2);  // at least time + tag1 + m1

    for (int i = 0; i < colCount; i++) {
        const char* colType = ts_dataset_get_column_type(dataSet, i);
        REQUIRE(colType != nullptr);
        REQUIRE(strlen(colType) > 0);
    }

    if (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        REQUIRE(record != nullptr);
        REQUIRE(ts_row_record_get_field_count(record) >= 1);
        (void)ts_row_record_get_timestamp(record);
        (void)ts_row_record_get_data_type(record, 0);
        (void)ts_row_record_is_null(record, 0);
        ts_row_record_destroy(record);
    }

    ts_dataset_destroy(dataSet);
}

/* ============================================================
 *  OBJECT column (requires server/table model OBJECT support)
 * ============================================================ */

TEST_CASE("C API Table - OBJECT tablet insert and read", "[c_table_object][c_table_write]") {
    CaseReporter cr("c_table_object");

    ts_table_session_execute_non_query(g_table_session, "DROP DATABASE IF EXISTS c_db_object");
    TsStatus status = ts_table_session_execute_non_query(g_table_session, "CREATE DATABASE c_db_object");
    REQUIRE(status == TS_OK);
    ts_table_session_execute_non_query(g_table_session, "USE \"c_db_object\"");
    status = ts_table_session_execute_non_query(
        g_table_session,
        "CREATE TABLE c_table_obj (tag1 string tag, payload object field)");
    REQUIRE(status == TS_OK);

    const char* columnNames[] = {"tag1", "payload"};
    TSDataType_C dataTypes[] = {TS_TYPE_STRING, TS_TYPE_OBJECT};
    TSColumnCategory_C colCategories[] = {TS_COL_TAG, TS_COL_FIELD};

    CTablet* tablet = ts_tablet_new_with_category("c_table_obj", 2, columnNames, dataTypes,
                                                    colCategories, 10);
    REQUIRE(tablet != nullptr);

    const uint8_t blob[] = {'h', 'e', 'l', 'l', 'o', '-', 'o', 'b', 'j'};
    ts_tablet_add_timestamp(tablet, 0, 1000LL);
    ts_tablet_add_value_string(tablet, 0, 0, "dev1");
    status = ts_tablet_add_value_object(tablet, 1, 0, true, 0, blob, sizeof(blob));
    REQUIRE(status == TS_OK);
    ts_tablet_set_row_count(tablet, 1);

    status = ts_table_session_insert(g_table_session, tablet);
    REQUIRE(status == TS_OK);
    ts_tablet_destroy(tablet);

    CSessionDataSet* dataSet = nullptr;
    status = ts_table_session_execute_query(g_table_session, "SELECT * FROM c_table_obj", &dataSet);
    REQUIRE(status == TS_OK);
    REQUIRE(dataSet != nullptr);

    REQUIRE(ts_dataset_has_next(dataSet));
    CRowRecord* record = ts_dataset_next(dataSet);
    REQUIRE(record != nullptr);

    bool foundObject = false;
    int n = ts_row_record_get_field_count(record);
    for (int i = 0; i < n; i++) {
        if (ts_row_record_get_data_type(record, i) == TS_TYPE_OBJECT) {
            foundObject = true;
            REQUIRE_FALSE(ts_row_record_is_null(record, i));
            const char* s = ts_row_record_get_string(record, i);
            REQUIRE(s != nullptr);
            /* OBJECT payloads are binary; do not use strlen (embedded NUL bytes are valid). */
            REQUIRE(ts_row_record_get_string_byte_length(record, i) > 0);
            break;
        }
    }
    REQUIRE(foundObject);

    ts_row_record_destroy(record);
    ts_dataset_destroy(dataSet);
}
