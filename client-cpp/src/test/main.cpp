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
#include "Session.h"

Session *session = new Session("127.0.0.1", 6667, "root", "root");

struct SessionListener : Catch::TestEventListenerBase {

    using TestEventListenerBase::TestEventListenerBase;

    void testCaseStarting(Catch::TestCaseInfo const &testInfo) override {
        // Perform some setup before a test case is run
        session->open(false);
    }

    void testCaseEnded(Catch::TestCaseStats const &testCaseStats) override {
        // Tear-down after a test case is run
        session->close();
    }
};

CATCH_REGISTER_LISTENER( SessionListener )