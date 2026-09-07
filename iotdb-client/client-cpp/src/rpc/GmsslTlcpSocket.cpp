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

#include "GmsslTlcpSocket.h"

#if defined(IOTDB_NTLS_PROVIDER_GMSSL)

#include <gmssl/socket.h>
#include <gmssl/version.h>

#include <sstream>
#include <string>
#include <utility>

using apache::thrift::transport::TTransportException;

GmsslTlcpSocket::GmsslTlcpSocket(const std::string& host, int port, SslConfig config)
    : TSocket(host, port), sslConfig_(std::move(config)) {
  RpcSslUtils::validateGmsslTlcpConfig(sslConfig_);
}

GmsslTlcpSocket::~GmsslTlcpSocket() {
  close();
}

void GmsslTlcpSocket::open() {
  if (connTimeout_ > 0) {
    if (recvTimeout_ <= 0) {
      setRecvTimeout(connTimeout_);
    }
    if (sendTimeout_ <= 0) {
      setSendTimeout(connTimeout_);
    }
  }
  TSocket::open();
  try {
    int result = tls_ctx_init(&tlsContext_, TLS_protocol_tlcp, 1);
    if (result != 1) {
      throwTransportError("GmSSL TLCP context initialization", result);
    }
    contextInitialized_ = true;
    RpcSslUtils::configureGmsslTlcpContext(&tlsContext_, sslConfig_);
    result = tls_init(&tlsConnection_, &tlsContext_);
    if (result != 1) {
      throwTransportError("GmSSL TLCP connection initialization", result);
    }
    connectionInitialized_ = true;
    result = tls_set_hostname(&tlsConnection_, getHost().c_str());
    if (result != 1) {
      throwTransportError("GmSSL TLCP hostname configuration", result);
    }
    result = tls_set_socket(&tlsConnection_, getSocketFD());
    if (result != 1) {
      throwTransportError("GmSSL TLCP socket configuration", result);
    }
    result = tls_do_handshake(&tlsConnection_);
    if (result != 1) {
      throwTransportError("GmSSL TLCP handshake", result);
    }
    handshakeComplete_ = true;
  } catch (...) {
    close();
    throw;
  }
}

void GmsslTlcpSocket::close() {
  if (connectionInitialized_) {
    if (handshakeComplete_) {
      (void)tls_shutdown(&tlsConnection_);
    }
    tls_client_verify_cleanup(&tlsConnection_.client_verify_ctx);
    tls_cleanup(&tlsConnection_);
    connectionInitialized_ = false;
    handshakeComplete_ = false;
    hasPeekedByte_ = false;
  }
  if (contextInitialized_) {
    tls_ctx_cleanup(&tlsContext_);
    contextInitialized_ = false;
  }
  TSocket::close();
}

bool GmsslTlcpSocket::peek() {
  if (hasPeekedByte_) {
    return true;
  }
  if (!handshakeComplete_) {
    throw TTransportException(TTransportException::NOT_OPEN, "GmSSL TLCP socket is not open");
  }
  size_t received = 0;
  const int result = tls_recv(&tlsConnection_, &peekedByte_, 1, &received);
  if (result == 1 && received == 1) {
    hasPeekedByte_ = true;
    return true;
  }
  if (result == 0 || result == TLS_ERROR_TCP_CLOSED) {
    return false;
  }
  throwTransportError("GmSSL TLCP peek", result);
}

uint32_t GmsslTlcpSocket::read(uint8_t* buf, uint32_t len) {
  if (!handshakeComplete_) {
    throw TTransportException(TTransportException::NOT_OPEN, "GmSSL TLCP socket is not open");
  }
  if (len == 0) {
    return 0;
  }
  if (hasPeekedByte_) {
    buf[0] = peekedByte_;
    hasPeekedByte_ = false;
    return 1;
  }
  size_t received = 0;
  const int result = tls_recv(&tlsConnection_, buf, len, &received);
  if (result == 1) {
    return static_cast<uint32_t>(received);
  }
  if (result == 0 || result == TLS_ERROR_TCP_CLOSED) {
    return 0;
  }
  throwTransportError("GmSSL TLCP read", result);
}

void GmsslTlcpSocket::write(const uint8_t* buf, uint32_t len) {
  uint32_t written = 0;
  while (written < len) {
    written += write_partial(buf + written, len - written);
  }
}

uint32_t GmsslTlcpSocket::write_partial(const uint8_t* buf, uint32_t len) {
  if (!handshakeComplete_) {
    throw TTransportException(TTransportException::NOT_OPEN, "GmSSL TLCP socket is not open");
  }
  size_t sent = 0;
  const int result = tls_send(&tlsConnection_, buf, len, &sent);
  if (result == 1 && sent > 0) {
    return static_cast<uint32_t>(sent);
  }
  throwTransportError("GmSSL TLCP write", result);
}

void GmsslTlcpSocket::throwTransportError(const std::string& operation, int result) const {
  const int socketError = tls_socket_get_error();
  const bool isRead = operation.find("read") != std::string::npos ||
                      operation.find("peek") != std::string::npos ||
                      operation.find("handshake") != std::string::npos;
  const tls_socket_err_t socketErrorType = tls_socket_get_error_type(socketError, isRead ? 1 : 0);
  std::ostringstream message;
  message << operation << " failed (GmSSL result=" << result;
  if (result == TLS_ERROR_RECV_AGAIN) {
    message << "/want-read";
  } else if (result == TLS_ERROR_SEND_AGAIN) {
    message << "/want-write";
  } else if (result == TLS_ERROR_TCP_CLOSED) {
    message << "/tcp-closed";
  } else if (result == TLS_ERROR_SYSCALL) {
    message << "/syscall";
  }
  if (tlsConnection_.protocol != 0) {
    message << ", protocol=" << tls_protocol_name(tlsConnection_.protocol);
  }
  if (tlsConnection_.cipher_suite != 0) {
    message << ", cipher=" << tls_cipher_suite_name(tlsConnection_.cipher_suite);
  }
  message << ", handshake_state=" << tlsConnection_.handshake_state
          << ", send_state=" << tlsConnection_.send_state
          << ", recv_state=" << tlsConnection_.recv_state
          << ", verify_result=" << tlsConnection_.verify_result << ", socket_error=" << socketError
          << "/" << tls_socket_get_error_string(socketError) << ", gmssl=" << gmssl_version_str()
          << "). GmSSL writes its detailed error trace to stderr.";
  throw TTransportException(socketErrorType == TLS_SOCKET_ERR_TIMEOUT
                                ? TTransportException::TIMED_OUT
                                : TTransportException::UNKNOWN,
                            message.str());
}

#endif
