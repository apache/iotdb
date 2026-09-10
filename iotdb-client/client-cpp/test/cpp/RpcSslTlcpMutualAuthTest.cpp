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

#include <cstdio>
#include <fstream>
#include <initializer_list>

#include "Common.h"
#include "RpcSslUtils.h"
#include "SslTestFixtures.h"

namespace {

bool fixtureExists(const std::string& path) {
  std::ifstream in(path.c_str(), std::ios::binary);
  return in.good();
}

void concatenate(const std::string& output, std::initializer_list<std::string> inputs) {
  std::ofstream out(output, std::ios::binary | std::ios::trunc);
  REQUIRE(out.good());
  for (const auto& input : inputs) {
    std::ifstream in(input, std::ios::binary);
    REQUIRE(in.good());
    out << in.rdbuf();
    out << '\n';
  }
}

struct TemporaryPemBundles {
  std::string certChain = "tongsuo-client-certs.pem";
  std::string privateKeys = "tongsuo-client-keys.pem";

  ~TemporaryPemBundles() {
    std::remove(certChain.c_str());
    std::remove(privateKeys.c_str());
  }
};

} // namespace

TEST_CASE("TLCP mutual auth creates client SSL_CTX from dual PKCS12", "[rpc][ssl][mutual]") {
#if WITH_SSL
  const std::string trustStore = ssltest::tlcpFixture("tlcp-trust.p12");
  REQUIRE(fixtureExists(trustStore));
  const std::string keyStore = ssltest::buildTlcpDualKeyStoreP12();
  REQUIRE_FALSE(keyStore.empty());
  REQUIRE(fixtureExists(keyStore));

  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLCP";
  config.trustStore = trustStore;
  config.trustStorePwd = ssltest::kStorePassword;
  config.keyStore = keyStore;
  config.keyStorePwd = ssltest::kStorePassword;

  REQUIRE_NOTHROW(RpcSslUtils::validateTrustStore(trustStore, config.trustStorePwd));
  REQUIRE_NOTHROW(RpcSslUtils::validateKeyStore(keyStore, config.keyStorePwd));

  SSL_CTX* ctx = RpcSslUtils::createClientSslContext(config);
  REQUIRE(ctx != nullptr);
  SSL_CTX_free(ctx);
#endif
}

TEST_CASE("TLCP mutual auth creates client SSL_CTX from PEM bundles", "[rpc][ssl][mutual]") {
#if WITH_SSL
  TemporaryPemBundles files;
  concatenate(files.certChain,
              {ssltest::tlcpFixture("client_sign.crt"), ssltest::tlcpFixture("client_enc.crt"),
               ssltest::tlcpFixture("ca.crt")});
  concatenate(files.privateKeys,
              {ssltest::tlcpFixture("client_sign.key"), ssltest::tlcpFixture("client_enc.key")});

  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLCP";
  config.trustStore = ssltest::tlcpFixture("ca.crt");
  config.tlcpCertChainFile = files.certChain;
  config.tlcpPrivateKeyFile = files.privateKeys;
  config.tlcpPrivateKeyPwd = ssltest::kStorePassword;

  SSL_CTX* ctx = RpcSslUtils::createClientSslContext(config);
  REQUIRE(ctx != nullptr);
  STACK_OF(X509)* certificateChain = nullptr;
  SSL_CTX_get_extra_chain_certs(ctx, &certificateChain);
  REQUIRE(certificateChain != nullptr);
  REQUIRE(sk_X509_num(certificateChain) == 1);
  SSL_CTX_free(ctx);
#endif
}
