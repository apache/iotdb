/*
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

#include <catch.hpp>

#include <cstdlib>
#include <memory>
#include <string>

#include "Session.h"
#include "SessionBuilder.h"
#include "SessionC.h"
#include "SessionDataSet.h"
#include "SessionPool.h"
#include "TableSessionBuilder.h"

namespace {

std::string fixture(const std::string& name) {
  return std::string(IOTDB_TEST_FIXTURES_DIR) + "/tls/" + name;
}

bool mutualTlsEnabled() {
  const char* value = std::getenv("IOTDB_CPP_SSL_MUTUAL_AUTH");
  return value != nullptr && std::string(value) == "1";
}

template <typename Builder> void configureTls(Builder& builder) {
  builder.useSSL(true)->trustCertFilePath(fixture("ca.crt"));
  if (mutualTlsEnabled()) {
    builder.clientCertificateFilePath(fixture("client.crt"))
        ->clientPrivateKeyFilePath(fixture("client.key"));
  }
}

SslConfig sslConfig() {
  SslConfig config;
  config.useSsl = true;
  config.trustCertFilePath = fixture("ca.crt");
  if (mutualTlsEnabled()) {
    config.clientCertificateFilePath = fixture("client.crt");
    config.clientPrivateKeyFilePath = fixture("client.key");
  }
  return config;
}

void requireDataSet(std::unique_ptr<SessionDataSet> dataSet) {
  REQUIRE(dataSet != nullptr);
  REQUIRE(dataSet->hasNext());
  REQUIRE(dataSet->next() != nullptr);
  dataSet->closeOperationHandle();
}

void requireCDataSet(CSessionDataSet* dataSet) {
  REQUIRE(dataSet != nullptr);
  REQUIRE(ts_dataset_has_next(dataSet));
  CRowRecord* row = ts_dataset_next(dataSet);
  REQUIRE(row != nullptr);
  ts_row_record_destroy(row);
  ts_dataset_destroy(dataSet);
}

} // namespace

TEST_CASE("C++ APIs communicate with a TLS-enabled IoTDB", "[tls]") {
  SessionBuilder treeBuilder;
  treeBuilder.host("127.0.0.1")->rpcPort(6667)->username("root")->password("root");
  configureTls(treeBuilder);
  auto treeSession = treeBuilder.build();
  requireDataSet(treeSession->executeQueryStatement("SHOW VERSION"));
  treeSession->close();

  TableSessionBuilder tableBuilder;
  tableBuilder.host("127.0.0.1")->rpcPort(6667)->username("root")->password("root");
  configureTls(tableBuilder);
  auto tableSession = tableBuilder.build();
  requireDataSet(tableSession->executeQueryStatement("SHOW VERSION"));
  tableSession->close();

  SessionPoolBuilder poolBuilder;
  poolBuilder.host("127.0.0.1")->rpcPort(6667)->username("root")->password("root");
  configureTls(poolBuilder);
  auto pool = poolBuilder.build();
  auto pooledDataSet = pool->executeQueryStatement("SHOW VERSION");
  REQUIRE(pooledDataSet->hasNext());
  REQUIRE(pooledDataSet->next() != nullptr);
  pool->close();
}

TEST_CASE("C APIs communicate with a TLS-enabled IoTDB", "[tls]") {
  const std::string certificatePath = fixture("client.crt");
  const std::string privateKeyPath = fixture("client.key");
  const char* cert = mutualTlsEnabled() ? certificatePath.c_str() : nullptr;
  const char* key = mutualTlsEnabled() ? privateKeyPath.c_str() : nullptr;

  CSession* treeSession = ts_session_new("127.0.0.1", 6667, "root", "root");
  REQUIRE(treeSession != nullptr);
  REQUIRE(ts_session_set_ssl_config(treeSession, fixture("ca.crt").c_str(), cert, key) == TS_OK);
  REQUIRE(ts_session_open(treeSession) == TS_OK);
  CSessionDataSet* treeDataSet = nullptr;
  REQUIRE(ts_session_execute_query(treeSession, "SHOW VERSION", &treeDataSet) == TS_OK);
  requireCDataSet(treeDataSet);
  REQUIRE(ts_session_close(treeSession) == TS_OK);
  ts_session_destroy(treeSession);

  CTableSession* tableSession = ts_table_session_new_with_ssl(
      "127.0.0.1", 6667, "root", "root", "", fixture("ca.crt").c_str(), cert, key);
  REQUIRE(tableSession != nullptr);
  CSessionDataSet* tableDataSet = nullptr;
  REQUIRE(ts_table_session_execute_query(tableSession, "SHOW VERSION", &tableDataSet) == TS_OK);
  requireCDataSet(tableDataSet);
  REQUIRE(ts_table_session_close(tableSession) == TS_OK);
  ts_table_session_destroy(tableSession);
}

TEST_CASE("TLS node discovery uses the final SSL configuration and supports failover", "[tls]") {
  std::vector<std::string> bootstrapNodes = {"127.0.0.1:1", "127.0.0.1:6667"};
  Session session(bootstrapNodes, "root", "root");
  session.setSslConfig(sslConfig());
  session.open();
  requireDataSet(session.executeQueryStatement("SHOW VERSION"));
  session.close();
}

TEST_CASE("mTLS server rejects a client without a certificate", "[mtls]") {
  SessionBuilder builder;
  builder.host("127.0.0.1")
      ->rpcPort(6667)
      ->username("root")
      ->password("root")
      ->useSSL(true)
      ->trustCertFilePath(fixture("ca.crt"));
  REQUIRE_THROWS(builder.build());
}
