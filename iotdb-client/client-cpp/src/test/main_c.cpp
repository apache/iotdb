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

#define CATCH_CONFIG_MAIN

#include <catch.hpp>
#include "SessionC.h"

// Global session handle used by the C API tree-model tests
CSession* g_session = nullptr;

struct CSessionListener : Catch::TestEventListenerBase {
    using TestEventListenerBase::TestEventListenerBase;

    void testCaseStarting(Catch::TestCaseInfo const& testInfo) override {
        g_session = ts_session_new("127.0.0.1", 6667, "root", "root");
        REQUIRE(g_session != nullptr);
        TsStatus st = ts_session_open(g_session);
        if (st != TS_OK) {
            ts_session_destroy(g_session);
            g_session = nullptr;
            FAIL("ts_session_open failed; ensure distribution is built and IoTDB listens on 127.0.0.1:6667");
        }
    }

    void testCaseEnded(Catch::TestCaseStats const& testCaseStats) override {
        if (g_session) {
            ts_session_close(g_session);
            ts_session_destroy(g_session);
            g_session = nullptr;
        }
    }
};

CATCH_REGISTER_LISTENER(CSessionListener)
