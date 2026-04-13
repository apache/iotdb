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
 * Tree model: create one timeseries, insert one row via string values, SELECT, cleanup.
 * Edit HOST / PORT / credentials below to match your IoTDB.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "SessionC.h"

#define HOST "127.0.0.1"
#define PORT 6667
#define USER "root"
#define PASS "root"

#define TS_PATH "root.cdemo.d0.s0"
#define DEVICE "root.cdemo.d0"

static void fail(const char* ctx, CSession* s) {
    fprintf(stderr, "[tree_example] %s failed: %s\n", ctx, ts_get_last_error());
    if (s) {
        ts_session_close(s);
        ts_session_destroy(s);
    }
    exit(1);
}

int main(void) {
    const char* path = TS_PATH;
    CSession* session = ts_session_new(HOST, PORT, USER, PASS);
    if (!session) {
        fprintf(stderr, "[tree_example] ts_session_new returned NULL: %s\n", ts_get_last_error());
        return 1;
    }
    if (ts_session_open(session) != TS_OK) {
        fail("ts_session_open", session);
    }

    bool exists = false;
    if (ts_session_check_timeseries_exists(session, path, &exists) != TS_OK) {
        fail("ts_session_check_timeseries_exists", session);
    }
    if (exists) {
        if (ts_session_delete_timeseries(session, path) != TS_OK) {
            fail("ts_session_delete_timeseries (cleanup old)", session);
        }
    }
    if (ts_session_create_timeseries(session, path, TS_TYPE_INT64, TS_ENCODING_RLE, TS_COMPRESSION_SNAPPY) !=
        TS_OK) {
        fail("ts_session_create_timeseries", session);
    }

    const char* measurements[] = {"s0"};
    const char* values[] = {"100"};
    if (ts_session_insert_record_str(session, DEVICE, 1LL, 1, measurements, values) != TS_OK) {
        fail("ts_session_insert_record_str", session);
    }

    CSessionDataSet* dataSet = NULL;
    if (ts_session_execute_query(session, "select s0 from root.cdemo.d0", &dataSet) != TS_OK) {
        fail("ts_session_execute_query", session);
    }
    if (!dataSet) {
        fprintf(stderr, "[tree_example] dataSet is NULL\n");
        ts_session_close(session);
        ts_session_destroy(session);
        return 1;
    }
    ts_dataset_set_fetch_size(dataSet, 1024);

    int rows = 0;
    while (ts_dataset_has_next(dataSet)) {
        CRowRecord* record = ts_dataset_next(dataSet);
        if (!record) {
            break;
        }
        int64_t v = ts_row_record_get_int64(record, 0);
        printf("[tree_example] row %d: s0 = %lld\n", rows, (long long)v);
        ts_row_record_destroy(record);
        rows++;
    }
    ts_dataset_destroy(dataSet);

    printf("[tree_example] done, read %d row(s).\n", rows);

    if (ts_session_delete_timeseries(session, path) != TS_OK) {
        fail("ts_session_delete_timeseries", session);
    }

    ts_session_close(session);
    ts_session_destroy(session);
    return 0;
}
