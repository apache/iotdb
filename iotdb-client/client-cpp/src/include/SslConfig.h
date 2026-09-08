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

#ifndef IOTDB_SSL_CONFIG_H
#define IOTDB_SSL_CONFIG_H

#include <string>

struct SslConfig {
  bool useSsl = false;
  std::string sslProtocol = "TLS";
  std::string trustStore;
  std::string trustStorePwd;
  std::string keyStore;
  std::string keyStorePwd;
  /** TLCP PEM client certificate chain; provider-specific ordering is documented in README. */
  std::string tlcpCertChainFile;
  /** TLCP PEM client private key bundle; provider-specific contents are documented in README. */
  std::string tlcpPrivateKeyFile;
  std::string tlcpPrivateKeyPwd;
  /** Legacy PEM trust certificate path; used when trustStore is empty. */
  std::string trustCertFilePath;

  std::string effectiveTrustStore() const;
};

#endif // IOTDB_SSL_CONFIG_H
