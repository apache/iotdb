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

#ifndef IOTDB_RPC_SSL_UTILS_H
#define IOTDB_RPC_SSL_UTILS_H

#include <memory>
#include <string>

#include "SslConfig.h"

#if WITH_SSL
#if defined(IOTDB_NTLS_PROVIDER_TONGSUO)
#include <openssl/ssl.h>
#include <thrift/transport/TSSLSocket.h>
#elif defined(IOTDB_NTLS_PROVIDER_GMSSL)
#include <gmssl/tls.h>
#endif
#endif

class RpcSslUtils {
public:
  static constexpr const char* DEFAULT_PROTOCOL = "TLS";
  static constexpr const char* DEFAULT_TLCP_CIPHER = "ECC-SM2-WITH-SM4-SM3";

  static void configure(const std::string& sslProtocol);
  static std::string getProtocol();

  static bool isTlcpProtocol(const std::string& protocol);
  static std::string normalizeProtocol(const std::string& value);
  static std::string resolveProtocol(const std::string& value);

  static void validateTrustStore(const std::string& trustStorePath,
                                 const std::string& trustStorePassword);
  static void validateKeyStore(const std::string& keyStorePath,
                               const std::string& keyStorePassword);

#if WITH_SSL
#if defined(IOTDB_NTLS_PROVIDER_TONGSUO)
  static SSL_CTX* createClientSslContext(const SslConfig& config);
  static std::shared_ptr<apache::thrift::transport::TSSLSocketFactory>
  createSslSocketFactory(const SslConfig& config);
#elif defined(IOTDB_NTLS_PROVIDER_GMSSL)
  static void validateGmsslTlcpConfig(const SslConfig& config);
  static void configureGmsslTlcpContext(TLS_CTX* context, const SslConfig& config);
#endif
#endif
};

#endif // IOTDB_RPC_SSL_UTILS_H
