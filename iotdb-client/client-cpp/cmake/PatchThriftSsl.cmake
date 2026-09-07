# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# =============================================================================
# PatchThriftSsl.cmake
#
# Extends the vendored Apache Thrift C++ SSL transport with SSLContextFactory and
# SSLContext(SSL_CTX*) so IoTDB can inject custom OpenSSL / NTLS contexts.
# =============================================================================

if(NOT WITH_SSL)
    return()
endif()

set(_thrift_ssl_header "${_thrift_src}/lib/cpp/src/thrift/transport/TSSLSocket.h")
set(_thrift_ssl_cpp "${_thrift_src}/lib/cpp/src/thrift/transport/TSSLSocket.cpp")
set(_thrift_ssl_patch_marker "${_thrift_root}/.patched-ssl-context-${THRIFT_VERSION}")

if(EXISTS "${_thrift_ssl_patch_marker}")
    return()
endif()

if(NOT EXISTS "${_thrift_ssl_header}")
    message(FATAL_ERROR "[Thrift] cannot patch missing ${_thrift_ssl_header}")
endif()

file(READ "${_thrift_ssl_header}" _thrift_ssl_header_content)
if(NOT _thrift_ssl_header_content MATCHES "SSLContextFactory")
    if(NOT _thrift_ssl_header_content MATCHES "#include <functional>")
        string(REPLACE
                "#include <string>"
                "#include <functional>\n#include <string>"
                _thrift_ssl_header_content "${_thrift_ssl_header_content}")
    endif()
    string(REPLACE
            "class SSLContext;"
            "class SSLContext;\ntypedef std::function<std::shared_ptr<SSLContext>()> SSLContextFactory;"
            _thrift_ssl_header_content "${_thrift_ssl_header_content}")
    string(REPLACE
            "  TSSLSocketFactory(SSLProtocol protocol = SSLTLS);"
            "  TSSLSocketFactory(SSLProtocol protocol = SSLTLS);\n  /**\n   * Constructor\n   *\n   * @param contextFactory Function invoked during construction to return a custom OpenSSL context.\n   */\n  TSSLSocketFactory(const SSLContextFactory& contextFactory);"
            _thrift_ssl_header_content "${_thrift_ssl_header_content}")
    string(REPLACE
            "  SSLContext(const SSLProtocol& protocol = SSLTLS);"
            "  SSLContext(const SSLProtocol& protocol = SSLTLS);\n  /**\n   * Wrap an existing OpenSSL SSL_CTX.\n   *\n   * Takes ownership of @a ctx; the caller must not call SSL_CTX_free on it.\n   */\n  explicit SSLContext(SSL_CTX* ctx);"
            _thrift_ssl_header_content "${_thrift_ssl_header_content}")
    file(WRITE "${_thrift_ssl_header}" "${_thrift_ssl_header_content}")
endif()

if(EXISTS "${_thrift_ssl_cpp}")
    file(READ "${_thrift_ssl_cpp}" _thrift_ssl_cpp_content)
    if(NOT _thrift_ssl_cpp_content MATCHES "SSLContext::SSLContext\\(SSL_CTX\\* ctx\\)")
        string(REPLACE
                "SSLContext::~SSLContext() {"
                "SSLContext::SSLContext(SSL_CTX* ctx) : ctx_(ctx) {\n  if (ctx_ == nullptr) {\n    string errors;\n    buildErrors(errors);\n    throw TSSLException(\"SSL_CTX_new: null context\");\n  }\n}\n\nSSLContext::~SSLContext() {"
                _thrift_ssl_cpp_content "${_thrift_ssl_cpp_content}")
        string(REPLACE
                "TSSLSocketFactory::TSSLSocketFactory(SSLProtocol protocol) : server_(false) {"
                "TSSLSocketFactory::TSSLSocketFactory(const SSLContextFactory& contextFactory) : server_(false) {\n  Guard guard(mutex_);\n  if (count_ == 0) {\n    if (!manualOpenSSLInitialization_) {\n      didWeInitializeOpenSSL_ = true;\n      initializeOpenSSL();\n    }\n    randomize();\n  }\n  count_++;\n  ctx_ = contextFactory();\n}\n\nTSSLSocketFactory::TSSLSocketFactory(SSLProtocol protocol) : server_(false) {"
                _thrift_ssl_cpp_content "${_thrift_ssl_cpp_content}")
        file(WRITE "${_thrift_ssl_cpp}" "${_thrift_ssl_cpp_content}")
    endif()
endif()

file(TOUCH "${_thrift_ssl_patch_marker}")
message(STATUS "[Thrift] applied SSLContextFactory patch to ${_thrift_ssl_header}")
