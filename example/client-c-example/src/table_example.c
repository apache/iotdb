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

/*
 * Table model: CREATE DATABASE/TABLE, insert rows via Tablet, SELECT, DROP DATABASE.
 * Requires IoTDB table-SQL support. Edit HOST / PORT / credentials below.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "SessionC.h"

#define HOST "127.0.0.1"
#define PORT 6667
#define USER "root"
#define PASS "root"

#define DB_NAME "cdemo_db"
#define TABLE_NAME "cdemo_t0"

static void fail(const char* ctx, CTableSession* s) {
    fprintf(stderr, "[table_example] %s failed: %s\n", ctx, ts_get_last_error());
    if (s) {
        ts_table_session_close(s);
        ts_table_session_destroy(s);
    }
    exit(1);
}

int main(void) {
    /* Last arg: default database name; empty string leaves it unset (we USE "cdemo_db" via SQL below). */
    CTableSession* session = ts_table_session_new(HOST, PORT, USER, PASS, "");
    if (!session) {
        fprintf(stderr, "[table_example] ts_table_session_new returned NULL: %s\n", ts_get_last_error());
        return 1;
    }
    if (ts_table_session_open(session) != TS_OK) {
        fail("ts_table_session_open", session);
    }

    char sql[512];
    snprintf(sql, sizeof(sql), "DROP DATABASE IF EXISTS %s", DB_NAME);
    (void)ts_table_session_execute_non_query(session, sql);

    snprintf(sql, sizeof(sql), "CREATE DATABASE %s", DB_NAME);
    if (ts_table_session_execute_non_query(session, sql) != TS_OK) {
        fail("CREATE DATABASE", session);
    }

    snprintf(sql, sizeof(sql), "USE \"%s\"", DB_NAME);
    if (ts_table_session_execute_non_query(session, sql) != TS_OK) {
        fail("USE DATABASE", session);
    }

    const char* ddl =
        "CREATE TABLE " TABLE_NAME " ("
        "tag1 string tag,"
        "attr1 string attribute,"
        "m1 double field)";
    if (ts_table_session_execute_non_query(session, ddl) != TS_OK) {
        fail("CREATE TABLE", session);
    }

    const char* columnNames[] = {"tag1", "attr1", "m1"};
    TSDataType_C dataTypes[] = {TS_TYPE_STRING, TS_TYPE_STRING, TS_TYPE_DOUBLE};
    TSColumnCategory_C colCategories[] = {TS_COL_TAG, TS_COL_ATTRIBUTE, TS_COL_FIELD};

    CTablet* tablet = ts_tablet_new_with_category(TABLE_NAME, 3, columnNames, dataTypes, colCategories, 100);
    if (!tablet) {
        fail("ts_tablet_new_with_category", session);
    }

    int i;
    for (i = 0; i < 5; i++) {
        if (ts_tablet_add_timestamp(tablet, i, (int64_t)i) != TS_OK) {
            ts_tablet_destroy(tablet);
            fail("ts_tablet_add_timestamp", session);
        }
        if (ts_tablet_add_value_string(tablet, 0, i, "device_A") != TS_OK) {
            ts_tablet_destroy(tablet);
            fail("ts_tablet_add_value_string tag", session);
        }
        if (ts_tablet_add_value_string(tablet, 1, i, "attr_val") != TS_OK) {
            ts_tablet_destroy(tablet);
            fail("ts_tablet_add_value_string attr", session);
        }
        if (ts_tablet_add_value_double(tablet, 2, i, (double)i * 1.5) != TS_OK) {
            ts_tablet_destroy(tablet);
            fail("ts_tablet_add_value_double", session);
        }
    }
    if (ts_tablet_set_row_count(tablet, 5) != TS_OK) {
        ts_tablet_destroy(tablet);
        fail("ts_tablet_set_row_count", session);
    }

    if (ts_table_session_insert(session, tablet) != TS_OK) {
        ts_tablet_destroy(tablet);
        fail("ts_table_session_insert", session);
    }
    ts_tablet_destroy(tablet);

    CSessionDataSet* dataSet = NULL;
    if (ts_table_session_execute_query(session, "SELECT * FROM " TABLE_NAME, &dataSet) != TS_OK) {
        fail("ts_table_session_execute_query", session);
    }
    if (!dataSet) {
        fprintf(stderr, "[table_example] dataSet is NULL\n");
        ts_table_session_close(session);
        ts_table_session_destroy(session);
        return 1;
    }
    ts_dataset_set_fetch_size(dataSet, 1024);

    int count = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        if (!record) {
            break;
        }
        printf("[table_example] row %d: time=%lld\n", count, (long long)ts_row_record_get_timestamp(record));
        ts_row_record_destroy(record);
        count++;
    }
    ts_dataset_destroy(dataSet);
    printf("[table_example] SELECT returned %d row(s).\n", count);

    snprintf(sql, sizeof(sql), "DROP DATABASE IF EXISTS %s", DB_NAME);
    (void)ts_table_session_execute_non_query(session, sql);

    ts_table_session_close(session);
    ts_table_session_destroy(session);
    return 0;
}
