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

#include "Common.h"
#include "RpcSslUtils.h"
#include "SslTestFixtures.h"

namespace {

bool fixtureExists(const std::string& path) {
  std::ifstream in(path.c_str(), std::ios::binary);
  return in.good();
}

} // namespace

TEST_CASE("TLS mutual auth creates client SSL_CTX with trust and key stores", "[rpc][ssl][mutual]") {
#if WITH_SSL
  const std::string trustStore = ssltest::tlsFixture("tls-trust.p12");
  const std::string keyStore = ssltest::tlsFixture("tls-client.p12");
  REQUIRE(fixtureExists(trustStore));
  REQUIRE(fixtureExists(keyStore));

  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLS";
  config.trustStore = trustStore;
  config.trustStorePwd = ssltest::kStorePassword;
  config.keyStore = keyStore;
  config.keyStorePwd = ssltest::kStorePassword;

  REQUIRE_NOTHROW(RpcSslUtils::validateTrustStore(trustStore, config.trustStorePwd));
  REQUIRE_NOTHROW(RpcSslUtils::validateKeyStore(keyStore, config.keyStorePwd));

  SSL_CTX* ctx = RpcSslUtils::createClientSslContext(config);
  REQUIRE(ctx != nullptr);
  REQUIRE(ssltest::sslContextHasClientCertificate(ctx));
  SSL_CTX_free(ctx);
#endif
}

TEST_CASE("TLS mutual auth handshake with openssl s_server", "[rpc][ssl][mutual][e2e]") {
#if WITH_SSL
  const std::string caFile = ssltest::tlsFixture("ca.crt");
  const std::string serverCert = ssltest::tlsFixture("server.crt");
  const std::string serverKey = ssltest::tlsFixture("server.key");
  REQUIRE(fixtureExists(caFile));
  REQUIRE(fixtureExists(serverCert));
  REQUIRE(fixtureExists(serverKey));

  ssltest::OpenSslServerProcess server;
  const bool started = server.start({
      "-tls1_2",
      "-Verify", "1",
      "-CAfile", caFile,
      "-cert", serverCert,
      "-key", serverKey,
      "-www",
  });
  REQUIRE(started);
  REQUIRE(server.running());
  REQUIRE(server.port() > 0);

  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLS";
  config.trustStore = ssltest::tlsFixture("tls-trust.p12");
  config.trustStorePwd = ssltest::kStorePassword;
  config.keyStore = ssltest::tlsFixture("tls-client.p12");
  config.keyStorePwd = ssltest::kStorePassword;

  REQUIRE(ssltest::tlsHandshakeWithSslConfig(config, "127.0.0.1", server.port()));
  server.stop();
#endif
}

TEST_CASE("TLS one-way auth fails when server requires client certificate", "[rpc][ssl][mutual][e2e]") {
#if WITH_SSL
  const std::string caFile = ssltest::tlsFixture("ca.crt");
  const std::string serverCert = ssltest::tlsFixture("server.crt");
  const std::string serverKey = ssltest::tlsFixture("server.key");
  REQUIRE(fixtureExists(caFile));

  ssltest::OpenSslServerProcess server;
  const bool started = server.start({
      "-tls1_2",
      "-Verify", "1",
      "-CAfile", caFile,
      "-cert", serverCert,
      "-key", serverKey,
      "-www",
  });
  REQUIRE(started);

  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLS";
  config.trustStore = ssltest::tlsFixture("tls-trust.p12");
  config.trustStorePwd = ssltest::kStorePassword;

  REQUIRE_FALSE(ssltest::tlsHandshakeWithSslConfig(config, "127.0.0.1", server.port()));
  server.stop();
#endif
}

TEST_CASE("Thrift TSSLSocketFactory verifies server certificate when trust store is set",
          "[rpc][ssl][thrift][e2e]") {
#if WITH_SSL
  const std::string caFile = ssltest::tlsFixture("ca.crt");
  const std::string serverCert = ssltest::tlsFixture("server.crt");
  const std::string serverKey = ssltest::tlsFixture("server.key");
  REQUIRE(fixtureExists(caFile));
  REQUIRE(fixtureExists(serverCert));
  REQUIRE(fixtureExists(serverKey));

  ssltest::OpenSslServerProcess server;
  const bool started = server.start({
      "-tls1_2",
      "-CAfile", caFile,
      "-cert", serverCert,
      "-key", serverKey,
      "-www",
  });
  REQUIRE(started);
  REQUIRE(server.port() > 0);

  SslConfig goodConfig;
  goodConfig.useSsl = true;
  goodConfig.sslProtocol = "TLS";
  goodConfig.trustStore = ssltest::tlsFixture("tls-trust.p12");
  goodConfig.trustStorePwd = ssltest::kStorePassword;
  REQUIRE(ssltest::thriftTlsHandshakeWithSslConfig(goodConfig, "127.0.0.1", server.port()));

  SslConfig badConfig = goodConfig;
  badConfig.trustStore = ssltest::tlsFixture("tls-client.p12");
  badConfig.keyStore.clear();
  REQUIRE_FALSE(ssltest::thriftTlsHandshakeWithSslConfig(badConfig, "127.0.0.1", server.port()));
  server.stop();
#endif
}

TEST_CASE("RpcSslUtils rejects JKS store paths", "[rpc][ssl]") {
#if WITH_SSL
  REQUIRE_THROWS_AS(RpcSslUtils::validateTrustStore("/path/to/trust.jks", "pwd"), IoTDBException);
  REQUIRE_THROWS_AS(RpcSslUtils::validateKeyStore("/path/to/client.jks", "pwd"), IoTDBException);
#endif
}
