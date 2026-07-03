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

#include <catch.hpp>

#include <fstream>
#include <memory>
#include <string>

#include "Common.h"
#include "RpcSslUtils.h"
#include "SslTestFixtures.h"

namespace {

bool fixtureExists(const std::string& path) {
  std::ifstream in(path.c_str(), std::ios::binary);
  return in.good();
}

SslConfig tlcpTrustOnlyConfig() {
  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLCP";
  config.trustStore = ssltest::tlcpFixture("tlcp-trust.p12");
  config.trustStorePwd = ssltest::kStorePassword;
  return config;
}

SslConfig tlcpMutualConfig() {
  SslConfig config = tlcpTrustOnlyConfig();
  config.keyStore = ssltest::buildTlcpDualKeyStoreP12();
  config.keyStorePwd = ssltest::kStorePassword;
  return config;
}

bool startTlcpServer(ssltest::OpenSslServerProcess& server, bool requireClientCert) {
  const std::string caFile = ssltest::tlcpFixture("ca.crt");
  const std::string signCert = ssltest::tlcpFixture("server_sign.crt");
  const std::string signKey = ssltest::tlcpFixture("server_sign.key");
  const std::string encCert = ssltest::tlcpFixture("server_enc.crt");
  const std::string encKey = ssltest::tlcpFixture("server_enc.key");
  if (!fixtureExists(caFile) || !fixtureExists(signCert) || !fixtureExists(signKey) ||
      !fixtureExists(encCert) || !fixtureExists(encKey)) {
    return false;
  }

  std::vector<std::string> args = {
      "-enable_ntls",
      "-ntls",
      "-CAfile", caFile,
      "-sign_cert", signCert,
      "-sign_key", signKey,
      "-enc_cert", encCert,
      "-enc_key", encKey,
      "-www",
  };
  if (requireClientCert) {
    args.push_back("-Verify");
    args.push_back("1");
  }
  return server.start(args) && server.running() && server.port() > 0;
}

} // namespace

TEST_CASE("TLCP one-way handshake with openssl NTLS s_server", "[rpc][ntls][e2e]") {
#if WITH_SSL
  ssltest::OpenSslServerProcess server;
  REQUIRE(startTlcpServer(server, false));
  REQUIRE(ssltest::tlsHandshakeWithSslConfig(tlcpTrustOnlyConfig(), "127.0.0.1", server.port()));
  server.stop();
#endif
}

TEST_CASE("TLCP one-way auth fails when server requires client certificate", "[rpc][ntls][e2e]") {
#if WITH_SSL
  ssltest::OpenSslServerProcess server;
  REQUIRE(startTlcpServer(server, true));
  REQUIRE_FALSE(ssltest::tlsHandshakeWithSslConfig(tlcpTrustOnlyConfig(), "127.0.0.1", server.port()));
  server.stop();
#endif
}

TEST_CASE("TLCP mutual auth handshake with dual PKCS12 client store", "[rpc][ntls][e2e]") {
#if WITH_SSL
  ssltest::OpenSslServerProcess server;
  REQUIRE(startTlcpServer(server, true));
  const SslConfig config = tlcpMutualConfig();
  REQUIRE_FALSE(config.keyStore.empty());
  REQUIRE(ssltest::tlsHandshakeWithSslConfig(config, "127.0.0.1", server.port()));
  SSL_CTX* ctx = RpcSslUtils::createClientSslContext(config);
  REQUIRE(ctx != nullptr);
  SSL_CTX_free(ctx);
  server.stop();
#endif
}
