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

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <process.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdlib>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#include <limits.h>
#endif
#endif

#if WITH_SSL
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/pkcs12.h>
#include <openssl/x509.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TTransportException.h>
#endif

#include "RpcSslUtils.h"
#include "SslTestFixtures.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <functional>
#include <sstream>
#include <thread>
#include <vector>

namespace ssltest {
namespace {

std::string joinPath(const std::string& base, const std::string& name) {
#if defined(_WIN32)
  const char sep = '\\';
#else
  const char sep = '/';
#endif
  if (base.empty()) {
    return name;
  }
  if (base.back() == '/' || base.back() == '\\') {
    return base + name;
  }
  return base + sep + name;
}

std::string executableDir() {
#if defined(_WIN32)
  char buffer[MAX_PATH];
  const DWORD len = GetModuleFileNameA(nullptr, buffer, MAX_PATH);
  if (len == 0 || len == MAX_PATH) {
    return ".";
  }
  std::string path(buffer, len);
  const auto pos = path.find_last_of("\\/");
  return pos == std::string::npos ? "." : path.substr(0, pos);
#elif defined(__APPLE__)
  char buffer[PATH_MAX];
  uint32_t size = sizeof(buffer);
  if (_NSGetExecutablePath(buffer, &size) != 0) {
    return ".";
  }
  std::string path(buffer);
  const auto pos = path.find_last_of('/');
  return pos == std::string::npos ? "." : path.substr(0, pos);
#else
  char buffer[4096];
  const ssize_t len = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
  if (len <= 0) {
    return ".";
  }
  buffer[len] = '\0';
  std::string path(buffer);
  const auto pos = path.find_last_of('/');
  return pos == std::string::npos ? "." : path.substr(0, pos);
#endif
}

bool pathExists(const std::string& path) {
  std::ifstream in(path.c_str(), std::ios::binary);
  return in.good();
}

std::string configuredOrEmpty() {
#ifdef IOTDB_TEST_FIXTURES_DIR
  return IOTDB_TEST_FIXTURES_DIR;
#else
  return joinPath(executableDir(), "fixtures");
#endif
}

std::string firstExistingRoot() {
#ifdef IOTDB_TEST_FIXTURES_DIR
  const std::string configured = IOTDB_TEST_FIXTURES_DIR;
  if (pathExists(joinPath(configured, "tls/tls-trust.p12")) ||
      pathExists(joinPath(configured, "tls\\tls-trust.p12"))) {
    return configured;
  }
#endif
  const std::string copied = joinPath(executableDir(), "fixtures");
  if (pathExists(joinPath(copied, "tls/tls-trust.p12")) ||
      pathExists(joinPath(copied, "tls\\tls-trust.p12"))) {
    return copied;
  }
  return configuredOrEmpty();
}

#if WITH_SSL
EVP_PKEY* readPrivateKeyPem(const std::string& path) {
  BIO* bio = BIO_new_file(path.c_str(), "rb");
  if (bio == nullptr) {
    return nullptr;
  }
  EVP_PKEY* key = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);
  return key;
}

X509* readCertificatePem(const std::string& path) {
  BIO* bio = BIO_new_file(path.c_str(), "rb");
  if (bio == nullptr) {
    return nullptr;
  }
  X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);
  return cert;
}

void addLocalKeyId(PKCS12_SAFEBAG* bag, X509* cert) {
  unsigned char keyid[EVP_MAX_MD_SIZE];
  unsigned int keyidLen = 0;
  if (X509_pubkey_digest(cert, EVP_sha1(), keyid, &keyidLen) == 1) {
    PKCS12_add_localkeyid(bag, keyid, static_cast<int>(keyidLen));
  }
}

void addCertAndKeyBags(STACK_OF(PKCS12_SAFEBAG)* bags, X509* cert, EVP_PKEY* key,
                       const char* friendlyName, const std::string& password) {
  PKCS12_SAFEBAG* certbag = PKCS12_SAFEBAG_create_cert(cert);
  PKCS12_add_friendlyname_utf8(certbag, friendlyName, -1);
  addLocalKeyId(certbag, cert);
  sk_PKCS12_SAFEBAG_push(bags, certbag);

  PKCS8_PRIV_KEY_INFO* p8 = EVP_PKEY2PKCS8(key);
  if (p8 == nullptr) {
    return;
  }
  PKCS12_SAFEBAG* keybag = PKCS12_SAFEBAG_create_pkcs8_encrypt(
      NID_pbes2, password.c_str(), static_cast<int>(password.size()), nullptr, 0, 2048, p8);
  PKCS8_PRIV_KEY_INFO_free(p8);
  if (keybag == nullptr) {
    return;
  }
  PKCS12_add_friendlyname_utf8(keybag, friendlyName, -1);
  addLocalKeyId(keybag, cert);
  sk_PKCS12_SAFEBAG_push(bags, keybag);
}

bool writePkcs12File(PKCS12* p12, const std::string& path) {
  BIO* bio = BIO_new_file(path.c_str(), "wb");
  if (bio == nullptr) {
    return false;
  }
  const int rc = i2d_PKCS12_bio(bio, p12);
  BIO_free(bio);
  return rc == 1;
}

PKCS12* readPkcs12File(const std::string& path) {
  BIO* bio = BIO_new_file(path.c_str(), "rb");
  if (bio == nullptr) {
    return nullptr;
  }
  PKCS12* p12 = d2i_PKCS12_bio(bio, nullptr);
  BIO_free(bio);
  return p12;
}

void forEachPkcs12Bag(PKCS12* p12, const std::string& password,
                      const std::function<void(PKCS12_SAFEBAG*)>& visitor) {
  STACK_OF(PKCS7)* safes = PKCS12_unpack_authsafes(p12);
  if (safes == nullptr) {
    return;
  }
  for (int i = 0; i < sk_PKCS7_num(safes); ++i) {
    PKCS7* p7 = sk_PKCS7_value(safes, i);
    STACK_OF(PKCS12_SAFEBAG)* bags = nullptr;
    if (PKCS7_type_is_data(p7)) {
      bags = PKCS12_unpack_p7data(p7);
    } else if (PKCS7_type_is_encrypted(p7)) {
      bags = PKCS12_unpack_p7encdata(p7, password.c_str(), static_cast<int>(password.size()));
    }
    if (bags == nullptr) {
      continue;
    }
    for (int j = 0; j < sk_PKCS12_SAFEBAG_num(bags); ++j) {
      visitor(sk_PKCS12_SAFEBAG_value(bags, j));
    }
    sk_PKCS12_SAFEBAG_pop_free(bags, PKCS12_SAFEBAG_free);
  }
  sk_PKCS7_pop_free(safes, PKCS7_free);
}

void appendPkcs12Bags(STACK_OF(PKCS12_SAFEBAG)* target, PKCS12* source,
                      const std::string& password) {
  forEachPkcs12Bag(source, password, [&](PKCS12_SAFEBAG* bag) {
    const int bagType = PKCS12_SAFEBAG_get_nid(bag);
    char* friendlyName = PKCS12_get_friendlyname(bag);
    if (bagType == NID_certBag) {
      X509* cert = PKCS12_certbag2x509(bag);
      if (cert != nullptr) {
        PKCS12_SAFEBAG* newBag = PKCS12_SAFEBAG_create_cert(cert);
        X509_free(cert);
        if (newBag != nullptr) {
          if (friendlyName != nullptr) {
            PKCS12_add_friendlyname_utf8(newBag, friendlyName, -1);
          }
          sk_PKCS12_SAFEBAG_push(target, newBag);
        }
      }
    } else if (bagType == NID_pkcs8ShroudedKeyBag || bagType == NID_keyBag) {
      EVP_PKEY* key = nullptr;
      if (bagType == NID_pkcs8ShroudedKeyBag) {
        PKCS8_PRIV_KEY_INFO* p8 = PKCS12_decrypt_skey(bag, password.c_str(),
                                                      static_cast<int>(password.size()));
        if (p8 != nullptr) {
          key = EVP_PKCS82PKEY(p8);
          PKCS8_PRIV_KEY_INFO_free(p8);
        }
      } else {
        const PKCS8_PRIV_KEY_INFO* p8 = PKCS12_SAFEBAG_get0_p8inf(bag);
        if (p8 != nullptr) {
          key = EVP_PKCS82PKEY(p8);
        }
      }
      if (key != nullptr) {
        PKCS8_PRIV_KEY_INFO* p8 = EVP_PKEY2PKCS8(key);
        EVP_PKEY_free(key);
        if (p8 != nullptr) {
          PKCS12_SAFEBAG* newBag = PKCS12_SAFEBAG_create_pkcs8_encrypt(
              NID_pbes2, password.c_str(), static_cast<int>(password.size()), nullptr, 0, 2048,
              p8);
          PKCS8_PRIV_KEY_INFO_free(p8);
          if (newBag != nullptr) {
            if (friendlyName != nullptr) {
              PKCS12_add_friendlyname_utf8(newBag, friendlyName, -1);
            }
            sk_PKCS12_SAFEBAG_push(target, newBag);
          }
        }
      }
    }
    if (friendlyName != nullptr) {
      OPENSSL_free(friendlyName);
    }
  });
}

std::string opensslExecutable() {
#ifdef IOTDB_OPENSSL_EXECUTABLE
  return IOTDB_OPENSSL_EXECUTABLE;
#else
  return "openssl";
#endif
}

#if !defined(_WIN32)
void prependOpenSslRuntimeToLdLibraryPath() {
#ifdef IOTDB_OPENSSL_ROOT_DIR
  const std::string root = IOTDB_OPENSSL_ROOT_DIR;
  std::string libPath = joinPath(root, "lib64");
  const std::string lib = joinPath(root, "lib");
  if (pathExists(lib)) {
    libPath = libPath + ":" + lib;
  }
  const char* existing = std::getenv("LD_LIBRARY_PATH");
  if (existing != nullptr && existing[0] != '\0') {
    libPath = libPath + ":" + existing;
  }
  setenv("LD_LIBRARY_PATH", libPath.c_str(), 1);
#if defined(__APPLE__)
  const char* dyldExisting = std::getenv("DYLD_LIBRARY_PATH");
  std::string dyldPath = libPath;
  if (dyldExisting != nullptr && dyldExisting[0] != '\0') {
    dyldPath = dyldPath + ":" + dyldExisting;
  }
  setenv("DYLD_LIBRARY_PATH", dyldPath.c_str(), 1);
#endif
#endif
}
#endif

std::string quoteArg(const std::string& arg) {
#if defined(_WIN32)
  return "\"" + arg + "\"";
#else
  if (arg.find(' ') != std::string::npos) {
    return "\"" + arg + "\"";
  }
  return arg;
#endif
}

} // namespace

std::string fixturesRoot() {
  return firstExistingRoot();
}

std::string tlsFixture(const std::string& name) {
  return joinPath(joinPath(fixturesRoot(), "tls"), name);
}

std::string tlcpFixture(const std::string& name) {
  return joinPath(joinPath(fixturesRoot(), "tlcp"), name);
}

std::string buildTlcpDualKeyStoreP12() {
  const std::string password = kStorePassword;
  const std::string outPath = joinPath(executableDir(), "tlcp-client-dual.p12");

  PKCS12* signStore = readPkcs12File(tlcpFixture("tlcp-client-sign.p12"));
  PKCS12* encStore = readPkcs12File(tlcpFixture("tlcp-client-enc.p12"));
  if (signStore == nullptr || encStore == nullptr) {
    if (signStore != nullptr) {
      PKCS12_free(signStore);
    }
    if (encStore != nullptr) {
      PKCS12_free(encStore);
    }
    return "";
  }

  STACK_OF(PKCS7)* safes = PKCS12_unpack_authsafes(signStore);
  STACK_OF(PKCS7)* encSafes = PKCS12_unpack_authsafes(encStore);
  PKCS12_free(signStore);
  PKCS12_free(encStore);
  if (safes == nullptr || encSafes == nullptr) {
    if (safes != nullptr) {
      sk_PKCS7_pop_free(safes, PKCS7_free);
    }
    if (encSafes != nullptr) {
      sk_PKCS7_pop_free(encSafes, PKCS7_free);
    }
    return "";
  }
  while (sk_PKCS7_num(encSafes) > 0) {
    PKCS7* p7 = sk_PKCS7_pop(encSafes);
    if (p7 == nullptr || sk_PKCS7_push(safes, p7) == 0) {
      PKCS7_free(p7);
      sk_PKCS7_pop_free(safes, PKCS7_free);
      sk_PKCS7_pop_free(encSafes, PKCS7_free);
      return "";
    }
  }
  sk_PKCS7_free(encSafes);

  PKCS12* p12 = PKCS12_init(NID_pkcs7_data);
  if (p12 == nullptr) {
    sk_PKCS7_pop_free(safes, PKCS7_free);
    return "";
  }
  if (PKCS12_pack_authsafes(p12, safes) != 1) {
    sk_PKCS7_pop_free(safes, PKCS7_free);
    PKCS12_free(p12);
    return "";
  }
  // PKCS12_pack_authsafes only encodes safes into p12; it does not take ownership.
  sk_PKCS7_pop_free(safes, PKCS7_free);

  const bool written = writePkcs12File(p12, outPath);
  PKCS12_free(p12);
  (void)password;
  return written ? outPath : "";
}

bool sslContextHasClientCertificate(SSL_CTX* ctx) {
  if (ctx == nullptr) {
    return false;
  }
  X509* cert = SSL_CTX_get0_certificate(ctx);
  EVP_PKEY* key = SSL_CTX_get0_privatekey(ctx);
  return cert != nullptr && key != nullptr;
}

bool tlsHandshakeWithSslConfig(const SslConfig& config, const std::string& host, int port,
                               int timeoutMs) {
#if defined(_WIN32)
  WSADATA wsaData;
  WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
  for (int attempt = 0; attempt < 3; ++attempt) {
    SSL_CTX* ctx = RpcSslUtils::createClientSslContext(config);
    if (ctx == nullptr) {
      continue;
    }
    SSL* ssl = SSL_new(ctx);
    if (ssl == nullptr) {
      SSL_CTX_free(ctx);
      continue;
    }
    if (RpcSslUtils::isTlcpProtocol(config.sslProtocol)) {
      RpcSslUtils::enableNtlsOnSsl(ssl);
    }
    const std::string target = host + ":" + std::to_string(port);
    BIO* bio = BIO_new_connect(target.c_str());
    if (bio == nullptr) {
      SSL_free(ssl);
      SSL_CTX_free(ctx);
      continue;
    }
    BIO_set_conn_hostname(bio, host.c_str());
    if (BIO_do_connect(bio) <= 0) {
      BIO_free_all(bio);
      SSL_free(ssl);
      SSL_CTX_free(ctx);
      continue;
    }
    SSL_set_bio(ssl, bio, bio);
    const int rc = SSL_connect(ssl);
    const bool ok = rc == 1;
    if (ok) {
      SSL_shutdown(ssl);
    }
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    if (ok) {
#if defined(_WIN32)
      WSACleanup();
#endif
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
#if defined(_WIN32)
  WSACleanup();
#endif
  (void)timeoutMs;
  return false;
}

bool thriftTlsHandshakeWithSslConfig(const SslConfig& config, const std::string& host, int port,
                                     int timeoutMs) {
  (void)timeoutMs;
  for (int attempt = 0; attempt < 3; ++attempt) {
    try {
      auto factory = RpcSslUtils::createSslSocketFactory(config);
      std::shared_ptr<apache::thrift::transport::TSSLSocket> socket =
          factory->createSocket(host, port);
      socket->open();
      socket->close();
      return true;
    } catch (const apache::thrift::transport::TTransportException&) {
      // Server may still be starting; retry below.
    } catch (const IoTDBException&) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return false;
}

int findFreeTcpPort() {
#if defined(_WIN32)
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
    return 0;
  }
#endif
  const int fd = static_cast<int>(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
  if (fd < 0) {
#if defined(_WIN32)
    WSACleanup();
#endif
    return 0;
  }
  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
#if defined(_WIN32)
    closesocket(fd);
    WSACleanup();
#else
    close(fd);
#endif
    return 0;
  }
  socklen_t len = sizeof(addr);
  if (getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
#if defined(_WIN32)
    closesocket(fd);
    WSACleanup();
#else
    close(fd);
#endif
    return 0;
  }
  const int port = ntohs(addr.sin_port);
#if defined(_WIN32)
  closesocket(fd);
  WSACleanup();
#else
  close(fd);
#endif
  return port;
}

OpenSslServerProcess::OpenSslServerProcess() = default;

OpenSslServerProcess::~OpenSslServerProcess() {
  stop();
}

bool OpenSslServerProcess::start(const std::vector<std::string>& args) {
  stop();
  port_ = findFreeTcpPort();
  if (port_ <= 0) {
    return false;
  }

  const std::string portArg = std::to_string(port_);
  const std::string exe = opensslExecutable();
  std::vector<std::string> argStorage;
  argStorage.reserve(args.size() + 5);
  argStorage.push_back(exe);
  argStorage.emplace_back("s_server");
  argStorage.emplace_back("-accept");
  argStorage.emplace_back(portArg);
  for (const std::string& arg : args) {
    argStorage.push_back(arg);
  }
  argStorage.emplace_back("-quiet");

  std::vector<const char*> argv;
  argv.reserve(argStorage.size() + 1);
  for (const std::string& arg : argStorage) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(nullptr);

#if defined(_WIN32)
  std::string cmdline = quoteArg(exe);
  for (size_t i = 1; i < argStorage.size(); ++i) {
    cmdline.push_back(' ');
    cmdline.append(quoteArg(argStorage[i]));
  }
  std::vector<char> mutableCmdline(cmdline.begin(), cmdline.end());
  mutableCmdline.push_back('\0');

  STARTUPINFOA si {};
  si.cb = sizeof(si);
  si.dwFlags = STARTF_USESHOWWINDOW;
  si.wShowWindow = SW_HIDE;
  PROCESS_INFORMATION pi {};
  if (!CreateProcessA(nullptr, mutableCmdline.data(), nullptr, nullptr, FALSE, CREATE_NO_WINDOW,
                      nullptr, nullptr, &si, &pi)) {
    return false;
  }
  processHandle_ = pi.hProcess;
  processId_ = pi.dwProcessId;
  CloseHandle(pi.hThread);
#else
  std::vector<char*> execArgv;
  execArgv.reserve(argStorage.size() + 1);
  for (std::string& arg : argStorage) {
    execArgv.push_back(const_cast<char*>(arg.c_str()));
  }
  execArgv.push_back(nullptr);
  const pid_t pid = fork();
  if (pid < 0) {
    return false;
  }
  if (pid == 0) {
    prependOpenSslRuntimeToLdLibraryPath();
    execv(exe.c_str(), execArgv.data());
    _exit(127);
  }
  childPid_ = pid;
#endif

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  return running();
}

void OpenSslServerProcess::stop() {
#if defined(_WIN32)
  if (processHandle_ != nullptr) {
    TerminateProcess(static_cast<HANDLE>(processHandle_), 0);
    WaitForSingleObject(static_cast<HANDLE>(processHandle_), 2000);
    CloseHandle(static_cast<HANDLE>(processHandle_));
    processHandle_ = nullptr;
    processId_ = 0;
  }
#else
  if (childPid_ > 0) {
    kill(childPid_, SIGTERM);
    waitpid(childPid_, nullptr, 0);
    childPid_ = -1;
  }
#endif
  port_ = 0;
}

bool OpenSslServerProcess::running() const {
#if defined(_WIN32)
  if (processHandle_ == nullptr) {
    return false;
  }
  DWORD code = STILL_ACTIVE;
  if (!GetExitCodeProcess(static_cast<HANDLE>(processHandle_), &code)) {
    return false;
  }
  return code == STILL_ACTIVE;
#else
  if (childPid_ <= 0) {
    return false;
  }
  int status = 0;
  const pid_t rc = waitpid(childPid_, &status, WNOHANG);
  return rc == 0;
#endif
}

int OpenSslServerProcess::port() const {
  return port_;
}

#else // WITH_SSL

std::string fixturesRoot() {
  return firstExistingRoot();
}

std::string tlsFixture(const std::string& name) {
  return joinPath(joinPath(fixturesRoot(), "tls"), name);
}

std::string tlcpFixture(const std::string& name) {
  return joinPath(joinPath(fixturesRoot(), "tlcp"), name);
}

std::string buildTlcpDualKeyStoreP12() {
  return "";
}

int findFreeTcpPort() {
  return 0;
}

OpenSslServerProcess::OpenSslServerProcess() = default;
OpenSslServerProcess::~OpenSslServerProcess() = default;
bool OpenSslServerProcess::start(const std::vector<std::string>&) {
  return false;
}
void OpenSslServerProcess::stop() {}
bool OpenSslServerProcess::running() const {
  return false;
}
int OpenSslServerProcess::port() const {
  return 0;
}

#endif // WITH_SSL

} // namespace ssltest
