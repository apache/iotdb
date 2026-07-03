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

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/pkcs12.h>
#include <openssl/x509.h>

#include <iostream>
#include <string>

namespace {

std::string joinPath(const std::string& base, const std::string& name) {
  if (base.empty()) {
    return name;
  }
  const char sep = (base.find('\\') != std::string::npos) ? '\\' : '/';
  if (base.back() == '/' || base.back() == '\\') {
    return base + name;
  }
  return base + sep + name;
}

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

} // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: gen_tlcp_dual_p12 <fixtures-tlcp-dir>\n";
    return 1;
  }
  OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, nullptr);
  const std::string dir = argv[1];
  const std::string password = "thrift";
  const std::string outPath = joinPath(dir, "tlcp-client-dual.p12");

  X509* signCert = readCertificatePem(joinPath(dir, "client_sign.crt"));
  EVP_PKEY* signKey = readPrivateKeyPem(joinPath(dir, "client_sign.key"));
  X509* encCert = readCertificatePem(joinPath(dir, "client_enc.crt"));
  EVP_PKEY* encKey = readPrivateKeyPem(joinPath(dir, "client_enc.key"));
  if (signCert == nullptr || signKey == nullptr || encCert == nullptr || encKey == nullptr) {
    std::cerr << "failed to read TLCP PEM fixtures\n";
    return 2;
  }

  STACK_OF(PKCS12_SAFEBAG)* bags = sk_PKCS12_SAFEBAG_new_null();
  addCertAndKeyBags(bags, signCert, signKey, "client.sign", password);
  addCertAndKeyBags(bags, encCert, encKey, "client.enc", password);
  PKCS7* p7 = PKCS12_pack_p7encdata(NID_pbes2, password.c_str(), static_cast<int>(password.size()),
                                    nullptr, 0, 2048, bags);
  sk_PKCS12_SAFEBAG_pop_free(bags, PKCS12_SAFEBAG_free);
  if (p7 == nullptr) {
    std::cerr << "failed to pack PKCS12 bags\n";
    return 3;
  }

  PKCS12* p12 = PKCS12_init(NID_pkcs7_data);
  STACK_OF(PKCS7)* safes = sk_PKCS7_new_null();
  sk_PKCS7_push(safes, p7);
  if (PKCS12_pack_authsafes(p12, safes) != 1) {
    sk_PKCS7_pop_free(safes, PKCS7_free);
    PKCS12_free(p12);
    std::cerr << "failed to pack PKCS12 authsafes\n";
    return 4;
  }
  sk_PKCS7_pop_free(safes, PKCS7_free);

  BIO* bio = BIO_new_file(outPath.c_str(), "wb");
  if (bio == nullptr || i2d_PKCS12_bio(bio, p12) != 1) {
  std::cerr << "failed to write " << outPath << "\n";
    BIO_free(bio);
    PKCS12_free(p12);
    return 5;
  }
  BIO_free(bio);
  PKCS12_free(p12);
  X509_free(signCert);
  EVP_PKEY_free(signKey);
  X509_free(encCert);
  EVP_PKEY_free(encKey);
  std::cout << "wrote " << outPath << "\n";
  return 0;
}
