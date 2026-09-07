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

#include "ExampleNtlsHandshake.h"

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

extern "C" int example_run_tlcp_handshake(void) {
#if !WITH_SSL
  return 1;
#else
  const std::string caFile = ssltest::tlcpFixture("ca.crt");
  const std::string signCert = ssltest::tlcpFixture("server_sign.crt");
  const std::string signKey = ssltest::tlcpFixture("server_sign.key");
  const std::string encCert = ssltest::tlcpFixture("server_enc.crt");
  const std::string encKey = ssltest::tlcpFixture("server_enc.key");
  if (!fixtureExists(caFile) || !fixtureExists(signCert) || !fixtureExists(signKey) ||
      !fixtureExists(encCert) || !fixtureExists(encKey)) {
    return 1;
  }

  ssltest::OpenSslServerProcess server;
  if (!server.start({
          "-enable_ntls",
          "-ntls",
          "-CAfile",
          caFile,
          "-sign_cert",
          signCert,
          "-sign_key",
          signKey,
          "-enc_cert",
          encCert,
          "-enc_key",
          encKey,
          "-www",
      })) {
    return 1;
  }

  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLCP";
  config.trustStore = ssltest::tlcpFixture("tlcp-trust.p12");
  config.trustStorePwd = ssltest::kStorePassword;

  const bool ok = ssltest::tlsHandshakeWithSslConfig(config, "127.0.0.1", server.port());
  server.stop();
  return ok ? 0 : 1;
#endif
}
