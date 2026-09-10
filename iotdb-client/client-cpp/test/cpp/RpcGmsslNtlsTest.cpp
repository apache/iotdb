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
#include "GmsslTlcpSocket.h"
#include "RpcSslUtils.h"

#if defined(_WIN32)
#include <winsock2.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <array>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include <thrift/transport/TBufferTransports.h>

namespace {

std::string fixture(const std::string& name) {
  return std::string(IOTDB_TEST_FIXTURES_DIR) + "/gmssl/" + name;
}

void initializeSockets() {
#if defined(_WIN32)
  static const bool winsockInitialized = [] {
    WSADATA data;
    return WSAStartup(MAKEWORD(2, 2), &data) == 0;
  }();
  REQUIRE(winsockInitialized);
#endif
}

void closeSocket(tls_socket_t socket) {
#if defined(_WIN32)
  closesocket(socket);
#else
  close(socket);
#endif
}

bool isValidSocket(tls_socket_t socket) {
#if defined(_WIN32)
  return socket != INVALID_SOCKET;
#else
  return socket >= 0;
#endif
}

void setSocketTimeout(tls_socket_t socket) {
#if defined(_WIN32)
  const DWORD timeout = 3000;
  setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout),
             sizeof(timeout));
  setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout),
             sizeof(timeout));
#else
  const timeval timeout{3, 0};
  setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif
}

class GmsslTestServer {
public:
  explicit GmsslTestServer(bool requireClientCertificate, bool exchangeFrame = false)
      : requireClientCertificate_(requireClientCertificate), exchangeFrame_(exchangeFrame) {
    initializeSockets();
    listener_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    REQUIRE(isValidSocket(listener_));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    REQUIRE(::bind(listener_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    REQUIRE(listen(listener_, 1) == 0);
#if defined(_WIN32)
    int length = sizeof(address);
#else
    socklen_t length = sizeof(address);
#endif
    REQUIRE(getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length) == 0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread(&GmsslTestServer::serve, this);
  }

  ~GmsslTestServer() {
    if (isValidSocket(listener_)) {
      closeSocket(listener_);
      listener_ = invalidSocket();
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const {
    return port_;
  }

  void finish() {
    if (thread_.joinable()) {
      thread_.join();
    }
    if (isValidSocket(listener_)) {
      closeSocket(listener_);
      listener_ = invalidSocket();
    }
    REQUIRE(serverError_.empty());
  }

private:
  static tls_socket_t invalidSocket() {
#if defined(_WIN32)
    return INVALID_SOCKET;
#else
    return -1;
#endif
  }

  static bool receiveAll(TLS_CONNECT* connection, uint8_t* data, size_t size) {
    size_t offset = 0;
    while (offset < size) {
      size_t received = 0;
      if (tls_recv(connection, data + offset, size - offset, &received) != 1 || received == 0) {
        return false;
      }
      offset += received;
    }
    return true;
  }

  static bool sendAll(TLS_CONNECT* connection, const uint8_t* data, size_t size) {
    size_t offset = 0;
    while (offset < size) {
      size_t sent = 0;
      if (tls_send(connection, data + offset, size - offset, &sent) != 1 || sent == 0) {
        return false;
      }
      offset += sent;
    }
    return true;
  }

  void serve() {
    tls_socket_t client = accept(listener_, nullptr, nullptr);
    if (!isValidSocket(client)) {
      serverError_ = "accept failed";
      return;
    }
    setSocketTimeout(client);

    TLS_CTX context{};
    TLS_CONNECT connection{};
    bool contextInitialized = false;
    bool connectionInitialized = false;
    bool handshakeComplete = false;
    const int cipherSuite = TLS_cipher_ecc_sm4_cbc_sm3;
    if (tls_ctx_init(&context, TLS_protocol_tlcp, 0) != 1) {
      serverError_ = "server context initialization failed";
      goto cleanup;
    }
    contextInitialized = true;
    if (tls_ctx_set_cipher_suites(&context, &cipherSuite, 1) != 1 ||
        tls_ctx_set_tlcp_server_certificate_and_keys(&context, fixture("server-certs.pem").c_str(),
                                                     fixture("server-keys.pem").c_str(),
                                                     "thrift") != 1) {
      serverError_ = "server credentials failed";
      goto cleanup;
    }
    if (requireClientCertificate_ &&
        tls_ctx_set_ca_certificates(&context, fixture("ca.crt").c_str(),
                                    TLS_DEFAULT_VERIFY_DEPTH) != 1) {
      serverError_ = "server CA configuration failed";
      goto cleanup;
    }
    if (tls_init(&connection, &context) != 1 || tls_set_socket(&connection, client) != 1) {
      serverError_ = "server connection initialization failed";
      goto cleanup;
    }
    connectionInitialized = true;
    if (tls_do_handshake(&connection) != 1) {
      serverError_ = "server handshake failed";
      goto cleanup;
    }
    handshakeComplete = true;
    if (exchangeFrame_) {
      std::array<uint8_t, 8> request{};
      if (!receiveAll(&connection, request.data(), request.size()) ||
          request != std::array<uint8_t, 8>{{0, 0, 0, 4, 'p', 'i', 'n', 'g'}}) {
        serverError_ = "server framed request mismatch";
        goto cleanup;
      }
      const std::array<uint8_t, 8> response{{0, 0, 0, 4, 'p', 'o', 'n', 'g'}};
      if (!sendAll(&connection, response.data(), response.size())) {
        serverError_ = "server framed response failed";
      }
    }

  cleanup:
    if (connectionInitialized) {
      if (handshakeComplete) {
        (void)tls_shutdown(&connection);
      }
      tls_client_verify_cleanup(&connection.client_verify_ctx);
      tls_cleanup(&connection);
    }
    if (contextInitialized) {
      tls_ctx_cleanup(&context);
    }
    closeSocket(client);
  }

  tls_socket_t listener_ = invalidSocket();
  std::thread thread_;
  std::string serverError_;
  bool requireClientCertificate_;
  bool exchangeFrame_;
  int port_;
};

SslConfig gmsslConfig(bool mutual) {
  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLCP";
  config.trustStore = fixture("ca.crt");
  if (mutual) {
    config.tlcpCertChainFile = fixture("client.crt");
    config.tlcpPrivateKeyFile = fixture("client.key");
    config.tlcpPrivateKeyPwd = "thrift";
  }
  return config;
}

} // namespace

TEST_CASE("GmSSL configures a one-way TLCP client context", "[rpc][ntls][gmssl]") {
  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLCP";
  config.trustStore = fixture("ca.crt");

  TLS_CTX context{};
  REQUIRE(tls_ctx_init(&context, TLS_protocol_tlcp, 1) == 1);
  REQUIRE_NOTHROW(RpcSslUtils::configureGmsslTlcpContext(&context, config));
  tls_ctx_cleanup(&context);
}

TEST_CASE("GmSSL loads mutual TLCP PEM bundles", "[rpc][ntls][gmssl]") {
  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLCP";
  config.trustStore = fixture("ca.crt");
  config.tlcpCertChainFile = fixture("client.crt");
  config.tlcpPrivateKeyFile = fixture("client.key");
  config.tlcpPrivateKeyPwd = "thrift";

  TLS_CTX context{};
  REQUIRE(tls_ctx_init(&context, TLS_protocol_tlcp, 1) == 1);
  REQUIRE_NOTHROW(RpcSslUtils::configureGmsslTlcpContext(&context, config));
  tls_ctx_cleanup(&context);
}

TEST_CASE("GmSSL rejects unsupported TLS and PKCS12 client stores", "[rpc][ntls][gmssl]") {
  SslConfig config;
  config.useSsl = true;
  config.sslProtocol = "TLS";
  REQUIRE_THROWS_WITH(RpcSslUtils::validateGmsslTlcpConfig(config),
                      Catch::Contains("supports TLCP only"));

  config.sslProtocol = "TLCP";
  config.keyStore = "client.p12";
  REQUIRE_THROWS_WITH(RpcSslUtils::validateGmsslTlcpConfig(config),
                      Catch::Contains("does not support PKCS12 keyStore"));
}

TEST_CASE("GmSSL native transport completes one-way TLCP handshake", "[rpc][ntls][gmssl][e2e]") {
  GmsslTestServer server(false);
  GmsslTlcpSocket socket("127.0.0.1", server.port(), gmsslConfig(false));
  socket.setConnTimeout(3000);
  REQUIRE_NOTHROW(socket.open());
  socket.close();
  server.finish();
}

TEST_CASE("GmSSL native transport completes mutual TLCP handshake", "[rpc][ntls][gmssl][e2e]") {
  GmsslTestServer server(true);
  GmsslTlcpSocket socket("127.0.0.1", server.port(), gmsslConfig(true));
  socket.setConnTimeout(3000);
  REQUIRE_NOTHROW(socket.open());
  socket.close();
  server.finish();
}

TEST_CASE("GmSSL transport exchanges a Thrift frame and peeks decrypted data",
          "[rpc][ntls][gmssl][e2e]") {
  GmsslTestServer server(false, true);
  auto socket = std::make_shared<GmsslTlcpSocket>("127.0.0.1", server.port(), gmsslConfig(false));
  socket->setConnTimeout(3000);
  apache::thrift::transport::TFramedTransport transport(socket);

  transport.open();
  const std::array<uint8_t, 4> request{{'p', 'i', 'n', 'g'}};
  transport.write(request.data(), request.size());
  transport.flush();
  REQUIRE(transport.peek());
  std::array<uint8_t, 4> response{};
  REQUIRE(transport.readAll(response.data(), response.size()) == response.size());
  const std::array<uint8_t, 4> expected{{'p', 'o', 'n', 'g'}};
  REQUIRE(response == expected);
  transport.close();
  server.finish();
}
