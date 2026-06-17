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
# FetchOpenSSL.cmake  (only included when WITH_SSL=ON)
#
# OpenSSL is pinned to the 1.x series. Apache Thrift 0.21's TSSLSocket.cpp uses
# APIs that were removed in OpenSSL 3.x (SSLv3_method / TLSv1_method /
# ASN1_STRING_data / non-const X509_NAME*), so a 3.x OpenSSL cannot be used.
#
# Resolution order:
#   1. find_package(OpenSSL) - accepted ONLY when it is 1.x. A 3.x system
#      OpenSSL is deliberately ignored (we do not fall back to it).
#   2. On Linux/macOS, when no usable 1.x OpenSSL is present:
#         use tarball ${IOTDB_OS_DEPS_DIR}/openssl-${OPENSSL_FALLBACK_VERSION}.tar.gz
#         or download from openssl.org when not in offline mode, then
#         ./config && make && make install_sw into ${CMAKE_BINARY_DIR}/_deps/openssl.
#   3. On Windows: emit a FATAL_ERROR asking for a prebuilt OpenSSL 1.1.1;
#      building OpenSSL from source on MSVC is out of scope.
#
# Side effects:
#   Defines imported targets OpenSSL::SSL / OpenSSL::Crypto via find_package
#   so callers can just link against them.
# =============================================================================

# Version built from source when no usable 1.x OpenSSL is found. Named distinctly
# from find_package's OPENSSL_VERSION output variable to avoid collisions.
set(OPENSSL_FALLBACK_VERSION "1.1.1w"
    CACHE STRING "OpenSSL 1.x version built from source when no usable system OpenSSL is found")

find_package(OpenSSL QUIET)
if(OpenSSL_FOUND AND OPENSSL_VERSION VERSION_GREATER_EQUAL "3.0")
    # Reject 3.x: Thrift 0.21 does not compile against it. Clear the resolved
    # state so the rest of the script treats it as "no usable OpenSSL".
    message(STATUS
            "[OpenSSL] ignoring system OpenSSL ${OPENSSL_VERSION}: Thrift "
            "${THRIFT_VERSION} requires OpenSSL 1.x. Will use "
            "${OPENSSL_FALLBACK_VERSION} instead.")
    set(OpenSSL_FOUND FALSE)
    unset(OPENSSL_INCLUDE_DIR CACHE)
    unset(OPENSSL_SSL_LIBRARY CACHE)
    unset(OPENSSL_CRYPTO_LIBRARY CACHE)
    unset(OPENSSL_SSL_LIBRARIES CACHE)
    unset(OPENSSL_CRYPTO_LIBRARIES CACHE)
    unset(SSL_EAY_RELEASE CACHE)
    unset(SSL_EAY_DEBUG CACHE)
    unset(LIB_EAY_RELEASE CACHE)
    unset(LIB_EAY_DEBUG CACHE)
endif()
if(OpenSSL_FOUND)
    message(STATUS "[OpenSSL] using system OpenSSL ${OPENSSL_VERSION} (1.x)")
    if(WIN32)
        # FindOpenSSL on the Shining Light / choco layout may pick an import lib
        # under lib/VC/<arch>/<MD|MT>/ that binds to a different OpenSSL ABI than
        # the bin/ DLLs. This happens on CI runners where a 1.1.1 install is laid
        # on top of a pre-existing 3.x one: the top-level lib/ and include/ become
        # 1.1.1 but the VC sub-dir import libs stay 3.x, so the link fails to
        # resolve 1.1-only symbols (e.g. SSL_get_peer_certificate). Pin the import
        # libs to the top-level lib/ ones, which match the 1.1 runtime DLLs we
        # bundle. Derive the root from the include dir find_package actually used.
        get_filename_component(_ossl_win_root "${OPENSSL_INCLUDE_DIR}" DIRECTORY)
        set(_ossl_ssl_implib    "${_ossl_win_root}/lib/libssl.lib")
        set(_ossl_crypto_implib "${_ossl_win_root}/lib/libcrypto.lib")
        if(EXISTS "${_ossl_ssl_implib}" AND EXISTS "${_ossl_crypto_implib}")
            message(STATUS
                    "[OpenSSL] pinning Windows import libs to ${_ossl_ssl_implib} "
                    "and ${_ossl_crypto_implib}")
            # FindOpenSSL creates these as UNKNOWN imported targets, which link via
            # IMPORTED_LOCATION_<CONFIG>; override both that and IMPORTED_IMPLIB for
            # every config so the pin applies regardless of how the target is typed.
            foreach(_cfg "" "_RELEASE" "_DEBUG" "_RELWITHDEBINFO" "_MINSIZEREL")
                set_target_properties(OpenSSL::SSL PROPERTIES
                        IMPORTED_LOCATION${_cfg} "${_ossl_ssl_implib}"
                        IMPORTED_IMPLIB${_cfg} "${_ossl_ssl_implib}")
                set_target_properties(OpenSSL::Crypto PROPERTIES
                        IMPORTED_LOCATION${_cfg} "${_ossl_crypto_implib}"
                        IMPORTED_IMPLIB${_cfg} "${_ossl_crypto_implib}")
            endforeach()
            set(OPENSSL_SSL_LIBRARY "${_ossl_ssl_implib}" CACHE FILEPATH "" FORCE)
            set(OPENSSL_CRYPTO_LIBRARY "${_ossl_crypto_implib}" CACHE FILEPATH "" FORCE)
        endif()
    endif()
    return()
endif()

if(WIN32)
    message(FATAL_ERROR
            "[OpenSSL] WITH_SSL=ON but no OpenSSL 1.x was found on Windows "
            "(Thrift ${THRIFT_VERSION} does not build against OpenSSL 3.x). "
            "Please install OpenSSL 1.1.1 (e.g. "
            "'choco install openssl --version=1.1.1.2100'), then re-run the "
            "configure step with -DOPENSSL_ROOT_DIR=<install_path>. Pass "
            "-DWITH_SSL=OFF to build without SSL.")
endif()

# --- Linux / macOS fallback: build OpenSSL 1.x from source ---------------
set(_ossl_tarname "openssl-${OPENSSL_FALLBACK_VERSION}.tar.gz")
set(_ossl_tarball "${IOTDB_OS_DEPS_DIR}/${_ossl_tarname}")

if(NOT EXISTS "${_ossl_tarball}")
    if(IOTDB_OFFLINE)
        message(FATAL_ERROR
                "[OpenSSL] IOTDB_OFFLINE=ON but ${_ossl_tarname} is missing in ${IOTDB_OS_DEPS_DIR}.")
    endif()
    set(_ossl_url "https://www.openssl.org/source/${_ossl_tarname}")
    message(STATUS "[OpenSSL] downloading ${_ossl_url}")
    file(DOWNLOAD "${_ossl_url}" "${_ossl_tarball}"
            SHOW_PROGRESS TLS_VERIFY ON STATUS _st)
    list(GET _st 0 _code)
    if(NOT _code EQUAL 0)
        list(GET _st 1 _msg)
        file(REMOVE "${_ossl_tarball}")
        message(FATAL_ERROR "[OpenSSL] download failed: ${_msg}")
    endif()
endif()

set(_ossl_root  "${CMAKE_BINARY_DIR}/_deps/openssl")
set(_ossl_src   "${_ossl_root}/src/openssl-${OPENSSL_FALLBACK_VERSION}")
set(_ossl_inst  "${_ossl_root}/install")
set(_ossl_stamp "${_ossl_root}/.built-${OPENSSL_FALLBACK_VERSION}")

if(NOT EXISTS "${_ossl_stamp}")
    file(REMOVE_RECURSE "${_ossl_root}/src")
    file(MAKE_DIRECTORY "${_ossl_root}/src")
    message(STATUS "[OpenSSL] extracting ${_ossl_tarball}")
    file(ARCHIVE_EXTRACT INPUT "${_ossl_tarball}" DESTINATION "${_ossl_root}/src")

    include(ProcessorCount)
    ProcessorCount(_jobs)
    if(_jobs LESS 1)
        set(_jobs 1)
    endif()

    message(STATUS "[OpenSSL] configuring -> ${_ossl_inst}")
    # ./config auto-detects the platform target (1.1.1's ./Configure needs an
    # explicit one); no-shared links OpenSSL straight into libiotdb_session.
    execute_process(
            COMMAND ./config --prefix=${_ossl_inst} --openssldir=${_ossl_inst}/ssl no-shared
            WORKING_DIRECTORY "${_ossl_src}"
            RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[OpenSSL] config failed (rc=${_rc})")
    endif()

    message(STATUS "[OpenSSL] building (-j${_jobs})")
    execute_process(
            COMMAND make -j${_jobs}
            WORKING_DIRECTORY "${_ossl_src}"
            RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[OpenSSL] make failed (rc=${_rc})")
    endif()

    execute_process(
            COMMAND make install_sw
            WORKING_DIRECTORY "${_ossl_src}"
            RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[OpenSSL] make install_sw failed (rc=${_rc})")
    endif()
    file(TOUCH "${_ossl_stamp}")
endif()

set(OPENSSL_ROOT_DIR "${_ossl_inst}" CACHE PATH "OpenSSL root" FORCE)
set(OPENSSL_USE_STATIC_LIBS ON)
find_package(OpenSSL REQUIRED)
message(STATUS "[OpenSSL] built locally at ${OPENSSL_ROOT_DIR}")
