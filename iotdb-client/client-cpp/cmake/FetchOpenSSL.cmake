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
# Apache Thrift 0.23 (bundled by this client) builds against OpenSSL 1.x and 3.x,
# so any system OpenSSL is used as-is, whatever its version.
#
# Resolution order:
#   1. find_package(OpenSSL) - any system / vendor install is taken as-is.
#   2. On Linux/macOS, when no system OpenSSL is present:
#         use tarball ${IOTDB_OS_DEPS_DIR}/openssl-${OPENSSL_FALLBACK_VERSION}.tar.gz
#         or download from openssl.org when not in offline mode, then
#         ./config && make && make install_sw into ${CMAKE_BINARY_DIR}/_deps/openssl.
#   3. On Windows: emit a FATAL_ERROR asking for a prebuilt OpenSSL; building
#      OpenSSL from source on MSVC is out of scope.
#
# Side effects:
#   Defines imported targets OpenSSL::SSL / OpenSSL::Crypto via find_package
#   so callers can just link against them.
# =============================================================================

# Version built from source when no system OpenSSL is found. Named distinctly
# from find_package's OPENSSL_VERSION output variable to avoid collisions.
set(OPENSSL_FALLBACK_VERSION "3.5.0"
    CACHE STRING "OpenSSL version built from source when no system OpenSSL is found")

# Build OpenSSL from source even if a system one exists. Used by the Linux
# packaging build, whose AlmaLinux 8 baseline ships OpenSSL 1.1.1 (EOL, not
# Apache-2.0, must not be redistributed) - we build 3.x there instead.
option(IOTDB_OPENSSL_FROM_SOURCE
        "Ignore any system OpenSSL and build OpenSSL ${OPENSSL_FALLBACK_VERSION} from source" OFF)

if(NOT IOTDB_OPENSSL_FROM_SOURCE)
    find_package(OpenSSL QUIET)
    if(OpenSSL_FOUND)
        message(STATUS "[OpenSSL] using system OpenSSL ${OPENSSL_VERSION}")
        return()
    endif()
endif()

if(WIN32)
    message(FATAL_ERROR
            "[OpenSSL] WITH_SSL=ON but no OpenSSL was found on Windows. "
            "Please install a prebuilt OpenSSL (e.g. 'choco install openssl'), "
            "then re-run the configure step with -DOPENSSL_ROOT_DIR=<install_path>. "
            "Pass -DWITH_SSL=OFF to build without SSL.")
endif()

# --- Linux / macOS: build OpenSSL ${OPENSSL_FALLBACK_VERSION} from source -
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
    # ./config auto-detects the platform target. Build SHARED libraries
    # (libssl.so.3 / libcrypto.so.3) so they can be bundled next to
    # libiotdb_session and shipped as the SDK's OpenSSL runtime.
    execute_process(
            COMMAND ./config --prefix=${_ossl_inst} --openssldir=${_ossl_inst}/ssl shared
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
set(OPENSSL_USE_STATIC_LIBS OFF)
find_package(OpenSSL REQUIRED)
message(STATUS "[OpenSSL] built locally (shared) at ${OPENSSL_ROOT_DIR}")
