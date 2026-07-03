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

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "ExampleTlsConfig.h"
#include "SessionC.h"

#define HOST "127.0.0.1"
#define PORT 6667
#define USER "root"
#define PASS "root"
#define TS_PATH "root.cdemo_tls.d0.s0"
#define DEVICE "root.cdemo_tls.d0"

static void fail(const char* ctx, CSession* s) {
  fprintf(stderr, "[tls_tree_example] %s failed: %s\n", ctx, ts_get_last_error());
  if (s) {
    ts_session_close(s);
    ts_session_destroy(s);
  }
  exit(1);
}

int main(void) {
  CSession* session = ts_session_new(HOST, PORT, USER, PASS);
  if (!session) {
    fprintf(stderr, "[tls_tree_example] ts_session_new returned NULL\n");
    return 1;
  }
  example_tls_configure_tree_session(session);
  if (ts_session_open(session) != TS_OK) {
    fail("ts_session_open", session);
  }

  bool exists = false;
  if (ts_session_check_timeseries_exists(session, TS_PATH, &exists) != TS_OK) {
    fail("ts_session_check_timeseries_exists", session);
  }
  if (exists) {
    (void)ts_session_delete_timeseries(session, TS_PATH);
  }
  if (ts_session_create_timeseries(session, TS_PATH, TS_TYPE_INT64, TS_ENCODING_RLE,
                                   TS_COMPRESSION_SNAPPY) != TS_OK) {
    fail("ts_session_create_timeseries", session);
  }

  const char* measurements[] = {"s0"};
  const char* values[] = {"100"};
  if (ts_session_insert_record_str(session, DEVICE, 1LL, 1, measurements, values) != TS_OK) {
    fail("ts_session_insert_record_str", session);
  }

  CSessionDataSet* dataSet = NULL;
  if (ts_session_execute_query(session, "select s0 from root.cdemo_tls.d0", &dataSet) != TS_OK) {
    fail("ts_session_execute_query", session);
  }
  if (!dataSet || !ts_dataset_has_next(dataSet)) {
    fail("ts_session_execute_query empty", session);
  }
  ts_dataset_destroy(dataSet);

  (void)ts_session_delete_timeseries(session, TS_PATH);
  ts_session_close(session);
  ts_session_destroy(session);
  printf("[tls_tree_example] ok\n");
  return 0;
}
