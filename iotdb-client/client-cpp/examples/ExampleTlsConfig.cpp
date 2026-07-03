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

#include "Session.h"
#include "TableSession.h"
#include "ExampleTlsConfig.h"

#include <fstream>
#include <string>

namespace {

constexpr const char* kStorePassword = "thrift";

std::string joinPath(const std::string& base, const std::string& name) {
#if defined(_WIN32)
  const char sep = '\\';
#else
  const char sep = '/';
#endif
  if (base.empty()) {
    return name;
  }
  if (base.back() == '/' || base.back() == '\\') {
    return base + name;
  }
  return base + sep + name;
}

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

std::string executableDir() {
#if defined(_WIN32)
  char buffer[MAX_PATH];
  const DWORD len = GetModuleFileNameA(nullptr, buffer, MAX_PATH);
  if (len == 0 || len == MAX_PATH) {
    return ".";
  }
  std::string path(buffer, len);
  const auto pos = path.find_last_of("\\/");
  return pos == std::string::npos ? "." : path.substr(0, pos);
#else
  char buffer[4096];
  const ssize_t len = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
  if (len <= 0) {
    return ".";
  }
  buffer[len] = '\0';
  std::string path(buffer);
  const auto pos = path.find_last_of('/');
  return pos == std::string::npos ? "." : path.substr(0, pos);
#endif
}

bool pathExists(const std::string& path) {
  std::ifstream in(path.c_str(), std::ios::binary);
  return in.good();
}

std::string trustStorePath() {
  static const std::string path = [] {
#ifdef IOTDB_TEST_FIXTURES_DIR
    const std::string configured = IOTDB_TEST_FIXTURES_DIR;
    const std::string configuredPath = joinPath(joinPath(configured, "tls"), "tls-trust.p12");
    if (pathExists(configuredPath)) {
      return configuredPath;
    }
#endif
    const std::string copied = joinPath(joinPath(executableDir(), "fixtures"), "tls/tls-trust.p12");
    return copied;
  }();
  return path;
}

} // namespace

extern "C" const char* example_tls_trust_store_path(void) {
  static std::string path = trustStorePath();
  return path.c_str();
}

extern "C" void example_tls_configure_tree_session(CSession* session) {
  if (session == nullptr) {
    return;
  }
  ts_session_set_use_ssl(session, true);
  ts_session_set_ssl_protocol(session, "TLS");
  ts_session_set_trust_store(session, example_tls_trust_store_path(), kStorePassword);
}

extern "C" void example_tls_configure_table_session(CTableSession* session) {
  if (session == nullptr) {
    return;
  }
  ts_table_session_set_use_ssl(session, true);
  ts_table_session_set_ssl_protocol(session, "TLS");
  ts_table_session_set_trust_store(session, example_tls_trust_store_path(), kStorePassword);
}

namespace examplessl {

void configureTreeSessionBuilder(SessionBuilder& builder) {
  builder.useSSL(true)
      ->sslProtocol("TLS")
      ->trustStore(trustStorePath())
      ->trustStorePwd(kStorePassword);
}

void configureTableSessionBuilder(TableSessionBuilder& builder) {
  builder.useSSL(true)
      ->sslProtocol("TLS")
      ->trustStore(trustStorePath())
      ->trustStorePwd(kStorePassword);
}

} // namespace examplessl
