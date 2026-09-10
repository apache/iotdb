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

#ifndef IOTDB_SSL_TEST_FIXTURES_H
#define IOTDB_SSL_TEST_FIXTURES_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace ssltest {

constexpr const char* kStorePassword = "thrift";

/** Root directory containing tls/ and tlcp/ fixture subfolders. */
std::string fixturesRoot();

std::string tlsFixture(const std::string& name);
std::string tlcpFixture(const std::string& name);

/** Build a TLCP dual-cert PKCS12 key store from PEM fixtures (sign + enc). */
std::string buildTlcpDualKeyStoreP12();

#if WITH_SSL
#include <openssl/ssl.h>

bool sslContextHasClientCertificate(SSL_CTX* ctx);
bool tlcpContextHasDualCredentials(SSL_CTX* ctx);

bool tlsHandshakeWithSslConfig(const SslConfig& config, const std::string& host, int port,
                               int timeoutMs = 5000);
#endif

/** Spawn bundled Tongsuo openssl s_server for integration-style handshake tests. */
class OpenSslServerProcess {
public:
  OpenSslServerProcess();
  ~OpenSslServerProcess();

  OpenSslServerProcess(const OpenSslServerProcess&) = delete;
  OpenSslServerProcess& operator=(const OpenSslServerProcess&) = delete;

  bool start(const std::vector<std::string>& args);
  void stop();
  bool running() const;
  int port() const;

private:
#if defined(_WIN32)
  void* processHandle_ = nullptr;
  unsigned long processId_ = 0;
#else
  int childPid_ = -1;
#endif
  int port_ = 0;
};

int findFreeTcpPort();

} // namespace ssltest

#endif // IOTDB_SSL_TEST_FIXTURES_H
