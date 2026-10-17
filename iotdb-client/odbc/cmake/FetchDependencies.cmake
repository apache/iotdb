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

include(FetchContent)

set(ODBC_CURL_VERSION "8.4.0" CACHE STRING "curl version used by the ODBC driver")
string(REPLACE "." "_" _odbc_curl_tag_version "${ODBC_CURL_VERSION}")
set(ODBC_CURL_URL
    "https://github.com/curl/curl/releases/download/curl-${_odbc_curl_tag_version}/curl-${ODBC_CURL_VERSION}.tar.xz"
    CACHE STRING "curl source archive")
set(ODBC_CURL_SHA256
    "16c62a9c4af0f703d28bda6d7bbf37ba47055ad3414d70dec63e2e6336f2a82d"
    CACHE STRING "curl source archive SHA-256")
set(ODBC_JSON_VERSION "3.11.3" CACHE STRING "nlohmann/json version used by the ODBC driver")
set(ODBC_JSON_URL
    "https://github.com/nlohmann/json/releases/download/v${ODBC_JSON_VERSION}/json.tar.xz"
    CACHE STRING "nlohmann/json source archive")
set(ODBC_JSON_SHA256
    "d6c65aca6b1ed68e7a182f4757257b107ae403032760ed6ef121c9d55e81757d"
    CACHE STRING "nlohmann/json source archive SHA-256")

# The REST transport only needs HTTP. Session TLS is provided by client-cpp.
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_CURL_EXE OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_LIBCURL_DOCS OFF CACHE BOOL "" FORCE)
set(ENABLE_MANUAL OFF CACHE BOOL "" FORCE)
set(HTTP_ONLY ON CACHE BOOL "" FORCE)
set(CURL_USE_OPENSSL OFF CACHE BOOL "" FORCE)
set(CURL_USE_SCHANNEL OFF CACHE BOOL "" FORCE)
set(CURL_ENABLE_SSL OFF CACHE BOOL "" FORCE)
set(CURL_ZLIB OFF CACHE BOOL "" FORCE)
set(CURL_BROTLI OFF CACHE BOOL "" FORCE)
set(CURL_ZSTD OFF CACHE BOOL "" FORCE)
set(CURL_USE_LIBPSL OFF CACHE BOOL "" FORCE)
set(CURL_USE_LIBSSH2 OFF CACHE BOOL "" FORCE)
set(USE_NGHTTP2 OFF CACHE BOOL "" FORCE)
set(USE_LIBIDN2 OFF CACHE BOOL "" FORCE)
set(ENABLE_LDAP OFF CACHE BOOL "" FORCE)
set(CURL_DISABLE_LDAP ON CACHE BOOL "" FORCE)
set(CURL_DISABLE_LDAPS ON CACHE BOOL "" FORCE)
set(CURL_STATIC_CRT ${MSVC} CACHE BOOL "" FORCE)
set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
set(JSON_Install OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    odbc_curl
    URL "${ODBC_CURL_URL}"
    URL_HASH "SHA256=${ODBC_CURL_SHA256}"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_Declare(
    odbc_json
    URL "${ODBC_JSON_URL}"
    URL_HASH "SHA256=${ODBC_JSON_SHA256}"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)

# CMake 3.20 does not yet support FetchContent's EXCLUDE_FROM_ALL argument.
# Populate explicitly so dependency targets remain buildable while their
# install rules do not leak headers, archives, or tools into the ODBC package.
if(POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif()

function(odbc_make_dependency_available dependency)
    FetchContent_GetProperties(${dependency})
    if(NOT ${dependency}_POPULATED)
        FetchContent_Populate(${dependency})
        add_subdirectory(
            "${${dependency}_SOURCE_DIR}"
            "${${dependency}_BINARY_DIR}"
            EXCLUDE_FROM_ALL)
    endif()
    set(${dependency}_SOURCE_DIR "${${dependency}_SOURCE_DIR}" PARENT_SCOPE)
endfunction()

odbc_make_dependency_available(odbc_curl)
odbc_make_dependency_available(odbc_json)
