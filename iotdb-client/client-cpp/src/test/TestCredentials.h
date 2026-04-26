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

#ifndef IOTDB_CLIENT_CPP_INTEGRATION_TEST_CREDENTIALS_H
#define IOTDB_CLIENT_CPP_INTEGRATION_TEST_CREDENTIALS_H

namespace iotdb {
namespace integration_test {

/** Default username for client-cpp integration tests (127.0.0.1:6667). */
constexpr const char* kUsername = "root";
/** Password; must match the server configuration under test. */
constexpr const char* kPassword = "TimechoDB@2021";
/** Deliberately wrong password for negative authentication tests. */
constexpr const char* kWrongPassword = "wrong-password";

} // namespace integration_test
} // namespace iotdb

#endif
