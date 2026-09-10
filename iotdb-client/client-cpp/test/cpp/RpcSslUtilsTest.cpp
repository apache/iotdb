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

#include "Common.h"
#include "RpcSslUtils.h"

TEST_CASE("RpcSslUtils protocol helpers", "[rpc][ssl]") {
  REQUIRE(RpcSslUtils::normalizeProtocol("") == "TLS");
  REQUIRE(RpcSslUtils::normalizeProtocol(" TLSv1.3 ") == "TLSv1.3");
  REQUIRE(RpcSslUtils::isTlcpProtocol("TLCP") == true);
  REQUIRE(RpcSslUtils::isTlcpProtocol(" tlcp1.1 ") == true);
  REQUIRE(RpcSslUtils::isTlcpProtocol("TLS") == false);

  const std::string origin = RpcSslUtils::getProtocol();
  RpcSslUtils::configure("ConfiguredProtocol");
  REQUIRE(RpcSslUtils::resolveProtocol("") == "ConfiguredProtocol");
  REQUIRE(RpcSslUtils::resolveProtocol(" ExplicitProtocol ") == "ExplicitProtocol");
  RpcSslUtils::configure(origin);
}

TEST_CASE("SslConfig effectiveTrustStore backward compatibility", "[rpc][ssl]") {
  SslConfig config;
  config.trustStore = "/path/to/trust.p12";
  config.trustCertFilePath = "/legacy/ca.pem";
  REQUIRE(config.effectiveTrustStore() == "/path/to/trust.p12");

  config.trustStore.clear();
  config.trustCertFilePath = "/legacy/ca.pem";
  REQUIRE(config.effectiveTrustStore() == "/legacy/ca.pem");
}

TEST_CASE("RpcSslUtils store validation rejects missing files", "[rpc][ssl]") {
  REQUIRE_THROWS_AS(RpcSslUtils::validateTrustStore("/path/does/not/exist.pem", ""),
                    IoTDBException);
  REQUIRE_THROWS_AS(RpcSslUtils::validateKeyStore("/path/does/not/exist.p12", "pwd"),
                    IoTDBException);
}

#if WITH_SSL && defined(IOTDB_NTLS_PROVIDER_TONGSUO)
TEST_CASE("RpcSslUtils createClientSslContext for TLS without trust store", "[rpc][ssl]") {
  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLS";
  SSL_CTX* ctx = RpcSslUtils::createClientSslContext(config);
  REQUIRE(ctx != nullptr);
  SSL_CTX_free(ctx);
}
#endif
