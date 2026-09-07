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

#ifndef IOTDB_GMSSL_TLCP_SOCKET_H
#define IOTDB_GMSSL_TLCP_SOCKET_H

#if defined(IOTDB_NTLS_PROVIDER_GMSSL)

#include "RpcSslUtils.h"

#include <gmssl/tls.h>
#include <thrift/transport/TSocket.h>

#include <cstdint>
#include <string>

class GmsslTlcpSocket : public apache::thrift::transport::TSocket {
public:
  GmsslTlcpSocket(const std::string& host, int port, SslConfig config);
  ~GmsslTlcpSocket() override;

  void open() override;
  void close() override;
  bool peek() override;
  uint32_t read(uint8_t* buf, uint32_t len) override;
  void write(const uint8_t* buf, uint32_t len) override;
  uint32_t write_partial(const uint8_t* buf, uint32_t len) override;

private:
  [[noreturn]] void throwTransportError(const std::string& operation, int result) const;

  SslConfig sslConfig_;
  TLS_CTX tlsContext_{};
  TLS_CONNECT tlsConnection_{};
  uint8_t peekedByte_ = 0;
  bool hasPeekedByte_ = false;
  bool contextInitialized_ = false;
  bool connectionInitialized_ = false;
  bool handshakeComplete_ = false;
};

#endif

#endif // IOTDB_GMSSL_TLCP_SOCKET_H
