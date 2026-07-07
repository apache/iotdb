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
#endif

#if WITH_SSL
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/pkcs12.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <thrift/transport/TSSLSocket.h>
#endif

#include "RpcSslUtils.h"

#include "Common.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <functional>
#include <vector>

namespace {

std::string gDefaultProtocol = RpcSslUtils::DEFAULT_PROTOCOL;

std::string trimToEmpty(const std::string& value) {
  const auto start = value.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(start, end - start + 1);
}

bool hasText(const std::string& value) {
  return !trimToEmpty(value).empty();
}

std::string toUpper(const std::string& value) {
  std::string out = value;
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return out;
}

bool endsWithIgnoreCase(const std::string& value, const std::string& suffix) {
  if (value.size() < suffix.size()) {
    return false;
  }
  const std::string tail = value.substr(value.size() - suffix.size());
  return toUpper(tail) == toUpper(suffix);
}

bool isPkcs12Path(const std::string& path) {
  return endsWithIgnoreCase(path, ".p12") || endsWithIgnoreCase(path, ".pfx");
}

bool isJksPath(const std::string& path) {
  return endsWithIgnoreCase(path, ".jks");
}

void rejectJksPath(const std::string& path, const std::string& label) {
  if (isJksPath(path)) {
    throw IoTDBException(label + " JKS is not supported by the C++ client; "
                                 "convert to PKCS#12 (.p12/.pfx) or use PEM");
  }
}

#if WITH_SSL

std::string collectOpenSslErrors() {
  std::string errors;
  unsigned long errCode = 0;
  while ((errCode = ERR_get_error()) != 0) {
    char buf[256];
    ERR_error_string_n(errCode, buf, sizeof(buf));
    if (!errors.empty()) {
      errors.append("; ");
    }
    errors.append(buf);
  }
  return errors.empty() ? "unknown OpenSSL error" : errors;
}

void throwSslError(const std::string& message) {
  throw IoTDBException(message + ": " + collectOpenSslErrors());
}

void ensureFileReadable(const std::string& path, const std::string& label) {
  if (!hasText(path)) {
    throw IoTDBException(label + " path is empty");
  }
  std::ifstream in(path.c_str(), std::ios::binary);
  if (!in.good()) {
    throw IoTDBException(label + " file not found: " + path);
  }
}

PKCS12* loadPkcs12(const std::string& path, const std::string& password) {
  BIO* bio = BIO_new_file(path.c_str(), "rb");
  if (bio == nullptr) {
    throwSslError("Failed to open PKCS12 file " + path);
  }
  PKCS12* p12 = d2i_PKCS12_bio(bio, nullptr);
  BIO_free(bio);
  if (p12 == nullptr) {
    throwSslError("Failed to parse PKCS12 file " + path);
  }
  (void)password;
  return p12;
}

struct Pkcs12ParsedIdentity {
  EVP_PKEY* pkey = nullptr;
  X509* cert = nullptr;
  STACK_OF(X509) * ca = nullptr;

  ~Pkcs12ParsedIdentity() {
    if (pkey != nullptr) {
      EVP_PKEY_free(pkey);
    }
    if (cert != nullptr) {
      X509_free(cert);
    }
    if (ca != nullptr) {
      sk_X509_pop_free(ca, X509_free);
    }
  }

  Pkcs12ParsedIdentity() = default;
  Pkcs12ParsedIdentity(const Pkcs12ParsedIdentity&) = delete;
  Pkcs12ParsedIdentity& operator=(const Pkcs12ParsedIdentity&) = delete;
};

void parsePkcs12OrThrow(PKCS12* p12, const std::string& password, Pkcs12ParsedIdentity& parsed,
                        const std::string& label) {
  if (PKCS12_parse(p12, password.empty() ? nullptr : password.c_str(), &parsed.pkey, &parsed.cert,
                   &parsed.ca) != 1) {
    throwSslError("Failed to parse PKCS12 " + label);
  }
}

std::string getBagFriendlyName(PKCS12_SAFEBAG* bag) {
  char* name = PKCS12_get_friendlyname(bag);
  if (name == nullptr) {
    return "";
  }
  std::string friendlyName(name);
  OPENSSL_free(name);
  return friendlyName;
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

EVP_PKEY* extractBagPrivateKey(PKCS12_SAFEBAG* bag, const std::string& password) {
  const int bagType = PKCS12_SAFEBAG_get_nid(bag);
  const PKCS8_PRIV_KEY_INFO* p8const = nullptr;
  PKCS8_PRIV_KEY_INFO* p8owned = nullptr;
  if (bagType == NID_pkcs8ShroudedKeyBag) {
    p8owned = PKCS12_decrypt_skey(bag, password.c_str(), static_cast<int>(password.size()));
    p8const = p8owned;
  } else if (bagType == NID_keyBag) {
    p8const = PKCS12_SAFEBAG_get0_p8inf(bag);
  }
  if (p8const == nullptr) {
    return nullptr;
  }
  EVP_PKEY* key = EVP_PKCS82PKEY(p8const);
  if (p8owned != nullptr) {
    PKCS8_PRIV_KEY_INFO_free(p8owned);
  }
  return key;
}

bool friendlyNameContains(const std::string& friendlyName, const std::string& keyword) {
  const std::string upperName = toUpper(friendlyName);
  const std::string upperKeyword = toUpper(keyword);
  return upperName.find(upperKeyword) != std::string::npos;
}

void validateCertificate(X509* cert) {
  if (cert == nullptr) {
    return;
  }
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  if (X509_cmp_current_time(X509_get0_notBefore(cert)) > 0 ||
      X509_cmp_current_time(X509_get0_notAfter(cert)) < 0) {
    throw IoTDBException("Certificate is not currently valid");
  }
#else
  if (X509_cmp_current_time(X509_get_notBefore(cert)) > 0 ||
      X509_cmp_current_time(X509_get_notAfter(cert)) < 0) {
    throw IoTDBException("Certificate is not currently valid");
  }
#endif
}

void addCertToStore(X509_STORE* store, X509* cert) {
  if (store == nullptr || cert == nullptr) {
    return;
  }
  if (X509_STORE_add_cert(store, cert) != 1) {
    const unsigned long errCode = ERR_peek_last_error();
    if (ERR_GET_LIB(errCode) != ERR_LIB_X509 ||
        ERR_GET_REASON(errCode) != X509_R_CERT_ALREADY_IN_HASH_TABLE) {
      throwSslError("Failed to add certificate to trust store");
    }
  }
}

void loadTrustFromPkcs12(SSL_CTX* ctx, const std::string& path, const std::string& password) {
  PKCS12* p12 = loadPkcs12(path, password);
  Pkcs12ParsedIdentity parsed;
  parsePkcs12OrThrow(p12, password, parsed, "trust store " + path);

  X509_STORE* store = SSL_CTX_get_cert_store(ctx);
  if (parsed.cert != nullptr) {
    validateCertificate(parsed.cert);
    addCertToStore(store, parsed.cert);
  }
  if (parsed.ca != nullptr) {
    for (int i = 0; i < sk_X509_num(parsed.ca); ++i) {
      X509* caCert = sk_X509_value(parsed.ca, i);
      validateCertificate(caCert);
      addCertToStore(store, caCert);
    }
  }

  forEachPkcs12Bag(p12, password, [&](PKCS12_SAFEBAG* bag) {
    if (PKCS12_SAFEBAG_get_nid(bag) == NID_certBag) {
      X509* bagCert = PKCS12_certbag2x509(bag);
      if (bagCert != nullptr) {
        validateCertificate(bagCert);
        addCertToStore(store, bagCert);
        X509_free(bagCert);
      }
    }
  });

  PKCS12_free(p12);
}

void loadTrustFromPem(SSL_CTX* ctx, const std::string& path) {
  if (SSL_CTX_load_verify_locations(ctx, path.c_str(), nullptr) != 1) {
    throwSslError("Failed to load PEM trust store " + path);
  }
}

void loadTrustStore(SSL_CTX* ctx, const std::string& path, const std::string& password) {
  rejectJksPath(path, "Trust store");
  ensureFileReadable(path, "Trust store");
  if (isPkcs12Path(path)) {
    loadTrustFromPkcs12(ctx, path, password);
  } else {
    loadTrustFromPem(ctx, path);
  }
}

void loadTlsIdentityFromPkcs12(SSL_CTX* ctx, const std::string& path, const std::string& password) {
  PKCS12* p12 = loadPkcs12(path, password);
  Pkcs12ParsedIdentity parsed;
  parsePkcs12OrThrow(p12, password, parsed, "key store " + path);
  PKCS12_free(p12);

  if (SSL_CTX_use_certificate(ctx, parsed.cert) != 1) {
    throwSslError("Failed to load client certificate from " + path);
  }
  if (SSL_CTX_use_PrivateKey(ctx, parsed.pkey) != 1) {
    throwSslError("Failed to load client private key from " + path);
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    throwSslError("Client certificate and private key do not match in " + path);
  }
}

void loadTlsIdentityFromPem(SSL_CTX* ctx, const std::string& path) {
  if (SSL_CTX_use_certificate_file(ctx, path.c_str(), SSL_FILETYPE_PEM) != 1) {
    throwSslError("Failed to load PEM client certificate from " + path);
  }
  if (SSL_CTX_use_PrivateKey_file(ctx, path.c_str(), SSL_FILETYPE_PEM) != 1) {
    throwSslError("Failed to load PEM client private key from " + path);
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    throwSslError("Client certificate and private key do not match in " + path);
  }
}

void loadTlsKeyStore(SSL_CTX* ctx, const std::string& path, const std::string& password) {
  rejectJksPath(path, "Key store");
  ensureFileReadable(path, "Key store");
  if (isPkcs12Path(path)) {
    loadTlsIdentityFromPkcs12(ctx, path, password);
  } else {
    loadTlsIdentityFromPem(ctx, path);
  }
}

struct TlcpIdentity {
  X509* signCert = nullptr;
  EVP_PKEY* signKey = nullptr;
  X509* encCert = nullptr;
  EVP_PKEY* encKey = nullptr;
};

void freeTlcpIdentity(TlcpIdentity& identity) {
  if (identity.signCert != nullptr) {
    X509_free(identity.signCert);
    identity.signCert = nullptr;
  }
  if (identity.signKey != nullptr) {
    EVP_PKEY_free(identity.signKey);
    identity.signKey = nullptr;
  }
  if (identity.encCert != nullptr) {
    X509_free(identity.encCert);
    identity.encCert = nullptr;
  }
  if (identity.encKey != nullptr) {
    EVP_PKEY_free(identity.encKey);
    identity.encKey = nullptr;
  }
}

void assignTlcpMaterial(TlcpIdentity& identity, const std::string& friendlyName, X509* cert,
                        EVP_PKEY* key) {
  if (friendlyNameContains(friendlyName, "enc")) {
    if (identity.encCert != nullptr) {
      X509_free(identity.encCert);
    }
    if (identity.encKey != nullptr) {
      EVP_PKEY_free(identity.encKey);
    }
    identity.encCert = cert;
    identity.encKey = key;
    return;
  }
  if (friendlyNameContains(friendlyName, "sign") || identity.signCert == nullptr) {
    if (identity.signCert != nullptr) {
      X509_free(identity.signCert);
    }
    if (identity.signKey != nullptr) {
      EVP_PKEY_free(identity.signKey);
    }
    identity.signCert = cert;
    identity.signKey = key;
    return;
  }
  if (identity.encCert == nullptr) {
    identity.encCert = cert;
    identity.encKey = key;
    return;
  }
  X509_free(cert);
  EVP_PKEY_free(key);
}

void loadTlcpKeyStoreFromPkcs12(SSL_CTX* ctx, const std::string& path,
                                const std::string& password) {
  PKCS12* p12 = loadPkcs12(path, password);
  TlcpIdentity identity;

  Pkcs12ParsedIdentity parsed;
  if (PKCS12_parse(p12, password.empty() ? nullptr : password.c_str(), &parsed.pkey, &parsed.cert,
                   &parsed.ca) == 1) {
    assignTlcpMaterial(identity, "sign", parsed.cert, parsed.pkey);
    parsed.cert = nullptr;
    parsed.pkey = nullptr;
  }

  forEachPkcs12Bag(p12, password, [&](PKCS12_SAFEBAG* bag) {
    const std::string friendlyName = getBagFriendlyName(bag);
    const int bagType = PKCS12_SAFEBAG_get_nid(bag);
    if (bagType == NID_certBag) {
      X509* cert = PKCS12_certbag2x509(bag);
      if (cert != nullptr) {
        if (friendlyNameContains(friendlyName, "enc")) {
          if (identity.encCert != nullptr) {
            X509_free(identity.encCert);
          }
          identity.encCert = cert;
        } else if (friendlyNameContains(friendlyName, "sign") || identity.signCert == nullptr) {
          if (identity.signCert != nullptr) {
            X509_free(identity.signCert);
          }
          identity.signCert = cert;
        } else if (identity.encCert == nullptr) {
          identity.encCert = cert;
        } else {
          X509_free(cert);
        }
      }
    } else if (bagType == NID_pkcs8ShroudedKeyBag || bagType == NID_keyBag) {
      EVP_PKEY* key = extractBagPrivateKey(bag, password);
      if (key != nullptr) {
        if (friendlyNameContains(friendlyName, "enc")) {
          if (identity.encKey != nullptr) {
            EVP_PKEY_free(identity.encKey);
          }
          identity.encKey = key;
        } else if (friendlyNameContains(friendlyName, "sign") || identity.signKey == nullptr) {
          if (identity.signKey != nullptr) {
            EVP_PKEY_free(identity.signKey);
          }
          identity.signKey = key;
        } else if (identity.encKey == nullptr) {
          identity.encKey = key;
        } else {
          EVP_PKEY_free(key);
        }
      }
    }
  });
  PKCS12_free(p12);

  if (identity.signCert == nullptr || identity.signKey == nullptr) {
    freeTlcpIdentity(identity);
    throw IoTDBException("TLCP PKCS12 key store must contain a signing certificate and key: " +
                         path);
  }

  if (SSL_CTX_use_sign_certificate(ctx, identity.signCert) != 1 ||
      SSL_CTX_use_sign_PrivateKey(ctx, identity.signKey) != 1) {
    freeTlcpIdentity(identity);
    throwSslError("Failed to load TLCP signing credentials from " + path);
  }

  if (identity.encCert != nullptr && identity.encKey != nullptr) {
    if (SSL_CTX_use_enc_certificate(ctx, identity.encCert) != 1 ||
        SSL_CTX_use_enc_PrivateKey(ctx, identity.encKey) != 1) {
      freeTlcpIdentity(identity);
      throwSslError("Failed to load TLCP encryption credentials from " + path);
    }
  }

  freeTlcpIdentity(identity);
}

void applyTlsProtocolVersion(SSL_CTX* ctx, const std::string& protocol) {
  const std::string resolved = RpcSslUtils::normalizeProtocol(protocol);
  const std::string upper = toUpper(resolved);
  if (upper == "TLSV1.2") {
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    SSL_CTX_set_max_proto_version(ctx, TLS1_2_VERSION);
    return;
  }
  if (upper == "TLSV1.3") {
    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
    SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION);
    return;
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
}

SSL_CTX* createTlsClientContext(const SslConfig& config) {
  const std::string protocol = RpcSslUtils::resolveProtocol(config.sslProtocol);
  SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
  if (ctx == nullptr) {
    throwSslError("Failed to create TLS client context");
  }
  applyTlsProtocolVersion(ctx, protocol);

  const std::string trustStore = config.effectiveTrustStore();
  if (hasText(trustStore)) {
    loadTrustStore(ctx, trustStore, config.trustStorePwd);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  }
  if (hasText(config.keyStore)) {
    loadTlsKeyStore(ctx, config.keyStore, config.keyStorePwd);
  }
  return ctx;
}

SSL_CTX* createTlcpClientContext(const SslConfig& config) {
  SSL_CTX* ctx = SSL_CTX_new(NTLS_client_method());
  if (ctx == nullptr) {
    throwSslError("Failed to create TLCP client context");
  }
  SSL_CTX_enable_ntls(ctx);
  if (SSL_CTX_set_cipher_list(ctx, RpcSslUtils::DEFAULT_TLCP_CIPHER) != 1) {
    SSL_CTX_free(ctx);
    throwSslError("Failed to set TLCP cipher suite");
  }

  const std::string trustStore = config.effectiveTrustStore();
  if (hasText(trustStore)) {
    loadTrustStore(ctx, trustStore, config.trustStorePwd);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  }
  if (hasText(config.keyStore)) {
    loadTlcpKeyStoreFromPkcs12(ctx, config.keyStore, config.keyStorePwd);
  }
  return ctx;
}

void validatePkcs12Store(const std::string& path, const std::string& password) {
  PKCS12* p12 = loadPkcs12(path, password);
  Pkcs12ParsedIdentity parsed;
  if (PKCS12_parse(p12, password.empty() ? nullptr : password.c_str(), &parsed.pkey, &parsed.cert,
                   &parsed.ca) != 1) {
    PKCS12_free(p12);
    throw IoTDBException("Failed to parse PKCS12 store: " + path);
  }
  if (parsed.cert != nullptr) {
    validateCertificate(parsed.cert);
  }
  if (parsed.ca != nullptr) {
    for (int i = 0; i < sk_X509_num(parsed.ca); ++i) {
      validateCertificate(sk_X509_value(parsed.ca, i));
    }
  }

  forEachPkcs12Bag(p12, password, [&](PKCS12_SAFEBAG* bag) {
    if (PKCS12_SAFEBAG_get_nid(bag) == NID_certBag) {
      X509* bagCert = PKCS12_certbag2x509(bag);
      if (bagCert != nullptr) {
        validateCertificate(bagCert);
        X509_free(bagCert);
      }
    }
  });
  PKCS12_free(p12);
}

void validatePemStore(const std::string& path) {
  BIO* bio = BIO_new_file(path.c_str(), "rb");
  if (bio == nullptr) {
    throw IoTDBException("Store file not found: " + path);
  }
  bool foundCert = false;
  while (true) {
    X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (cert == nullptr) {
      break;
    }
    validateCertificate(cert);
    X509_free(cert);
    foundCert = true;
  }
  BIO_free(bio);
  if (!foundCert) {
    throw IoTDBException("No valid certificate found in PEM store: " + path);
  }
}

#endif // WITH_SSL

} // namespace

std::string SslConfig::effectiveTrustStore() const {
  if (hasText(trustStore)) {
    return trimToEmpty(trustStore);
  }
  return trimToEmpty(trustCertFilePath);
}

void RpcSslUtils::configure(const std::string& sslProtocol) {
  gDefaultProtocol = normalizeProtocol(sslProtocol);
}

std::string RpcSslUtils::getProtocol() {
  return gDefaultProtocol;
}

bool RpcSslUtils::isTlcpProtocol(const std::string& protocol) {
  return toUpper(trimToEmpty(protocol)).find("TLCP") == 0;
}

std::string RpcSslUtils::normalizeProtocol(const std::string& value) {
  const std::string trimmed = trimToEmpty(value);
  return trimmed.empty() ? DEFAULT_PROTOCOL : trimmed;
}

std::string RpcSslUtils::resolveProtocol(const std::string& value) {
  const std::string trimmed = trimToEmpty(value);
  return trimmed.empty() ? gDefaultProtocol : trimmed;
}

void RpcSslUtils::validateTrustStore(const std::string& trustStorePath,
                                     const std::string& trustStorePassword) {
#if WITH_SSL
  rejectJksPath(trustStorePath, "Trust store");
  ensureFileReadable(trustStorePath, "Trust store");
  if (isPkcs12Path(trustStorePath)) {
    validatePkcs12Store(trustStorePath, trustStorePassword);
  } else {
    validatePemStore(trustStorePath);
  }
#else
  (void)trustStorePath;
  (void)trustStorePassword;
  throw IoTDBException("SSL/TLS support is not enabled in this build.");
#endif
}

void RpcSslUtils::validateKeyStore(const std::string& keyStorePath,
                                   const std::string& keyStorePassword) {
#if WITH_SSL
  rejectJksPath(keyStorePath, "Key store");
  ensureFileReadable(keyStorePath, "Key store");
  if (isPkcs12Path(keyStorePath)) {
    validatePkcs12Store(keyStorePath, keyStorePassword);
  } else {
    validatePemStore(keyStorePath);
  }
#else
  (void)keyStorePath;
  (void)keyStorePassword;
  throw IoTDBException("SSL/TLS support is not enabled in this build.");
#endif
}

#if WITH_SSL

void RpcSslUtils::enableNtlsOnSsl(SSL* ssl) {
  if (ssl != nullptr) {
    SSL_enable_ntls(ssl);
  }
}

SSL_CTX* RpcSslUtils::createClientSslContext(const SslConfig& config) {
  const std::string protocol = resolveProtocol(config.sslProtocol);
  if (isTlcpProtocol(protocol)) {
    return createTlcpClientContext(config);
  }
  return createTlsClientContext(config);
}

std::shared_ptr<apache::thrift::transport::TSSLSocketFactory>
RpcSslUtils::createSslSocketFactory(const SslConfig& config) {
  auto sslConfig = std::make_shared<SslConfig>(config);
  auto factory = std::make_shared<apache::thrift::transport::TSSLSocketFactory>(
      [sslConfig]() -> std::shared_ptr<apache::thrift::transport::SSLContext> {
        SSL_CTX* ctx = createClientSslContext(*sslConfig);
        return std::make_shared<apache::thrift::transport::SSLContext>(ctx);
      });
  factory->authenticate(!config.effectiveTrustStore().empty());
  return factory;
}

#endif
