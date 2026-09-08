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
# Apache Thrift 0.24 (bundled by this client) builds against OpenSSL 3.x.
#
# By default, a fixed OpenSSL source release is downloaded, checksum-verified,
# built as shared libraries, and installed under ${CMAKE_BINARY_DIR}/_deps.
# Set IOTDB_OPENSSL_FROM_SOURCE=OFF to opt into a compatible system OpenSSL.
#
# Side effects:
#   Defines imported targets OpenSSL::SSL / OpenSSL::Crypto via find_package
#   so callers can just link against them.
# =============================================================================

# Version built from source by default. Named distinctly
# from find_package's OPENSSL_VERSION output variable to avoid collisions.
set(OPENSSL_FALLBACK_VERSION "3.5.8"
    CACHE STRING "OpenSSL version built from source when no system OpenSSL is found")
set(OPENSSL_FALLBACK_SHA256
    "a8f84a39918ec6415ce765d9b429d313ba97b8143169c172e734b9514464f5b2"
    CACHE STRING "SHA-256 checksum of the pinned OpenSSL source archive")

# Build OpenSSL from source even if a system one exists, making release
# packages independent of the build host's OpenSSL installation.
option(IOTDB_OPENSSL_FROM_SOURCE
        "Ignore any system OpenSSL and build OpenSSL ${OPENSSL_FALLBACK_VERSION} from source" ON)

if(NOT IOTDB_OPENSSL_FROM_SOURCE)
    find_package(OpenSSL QUIET)
    if(OpenSSL_FOUND)
        message(STATUS "[OpenSSL] using system OpenSSL ${OPENSSL_VERSION}")
        return()
    endif()
endif()

# --- Build the pinned OpenSSL source release ---------------------------------
set(_ossl_tarname "openssl-${OPENSSL_FALLBACK_VERSION}.tar.gz")
set(_ossl_tarball "${IOTDB_OS_DEPS_DIR}/${_ossl_tarname}")

set(_ossl_download_required ON)
if(EXISTS "${_ossl_tarball}")
    file(SHA256 "${_ossl_tarball}" _ossl_existing_sha256)
    if(_ossl_existing_sha256 STREQUAL "${OPENSSL_FALLBACK_SHA256}")
        set(_ossl_download_required OFF)
    elseif(IOTDB_OFFLINE)
        message(FATAL_ERROR
                "[OpenSSL] checksum mismatch for offline archive ${_ossl_tarball}: "
                "expected ${OPENSSL_FALLBACK_SHA256}, got ${_ossl_existing_sha256}")
    else()
        message(STATUS "[OpenSSL] replacing archive with an invalid checksum")
        file(REMOVE "${_ossl_tarball}")
    endif()
endif()

if(_ossl_download_required)
    if(IOTDB_OFFLINE)
        message(FATAL_ERROR
                "[OpenSSL] IOTDB_OFFLINE=ON but ${_ossl_tarname} is missing in ${IOTDB_OS_DEPS_DIR}.")
    endif()
    set(_ossl_url
        "https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_FALLBACK_VERSION}/${_ossl_tarname}")
    message(STATUS "[OpenSSL] downloading ${_ossl_url}")
    file(DOWNLOAD "${_ossl_url}" "${_ossl_tarball}"
            SHOW_PROGRESS TLS_VERIFY ON
            EXPECTED_HASH "SHA256=${OPENSSL_FALLBACK_SHA256}" STATUS _st)
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

    message(STATUS "[OpenSSL] configuring -> ${_ossl_inst}")
    if(WIN32)
        # Git for Windows also ships a minimal Perl, but it lacks modules required by OpenSSL.
        find_program(_ossl_perl NAMES perl.exe perl
                PATHS "C:/Strawberry/perl/bin" NO_DEFAULT_PATH)
        if(NOT _ossl_perl)
            find_program(_ossl_perl NAMES perl.exe perl REQUIRED)
        endif()
        find_program(_vswhere NAMES vswhere.exe
                PATHS "$ENV{ProgramFiles}/Microsoft Visual Studio/Installer"
                      "C:/Program Files (x86)/Microsoft Visual Studio/Installer")
        if(NOT _vswhere)
            message(FATAL_ERROR "[OpenSSL] vswhere.exe was not found")
        endif()
        if(CMAKE_GENERATOR MATCHES "Visual Studio ([0-9]+)")
            set(_vs_major "${CMAKE_MATCH_1}")
            math(EXPR _vs_next_major "${_vs_major} + 1")
            set(_vs_range "[${_vs_major}.0,${_vs_next_major}.0)")
        else()
            set(_vs_range "[15.0,19.0)")
        endif()
        execute_process(
                COMMAND "${_vswhere}" -latest -products * -version "${_vs_range}"
                        -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64
                        -property installationPath
                OUTPUT_VARIABLE _vs_install OUTPUT_STRIP_TRAILING_WHITESPACE
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0 OR NOT _vs_install)
            message(FATAL_ERROR "[OpenSSL] a matching Visual Studio C++ toolchain was not found")
        endif()
        file(TO_NATIVE_PATH "${_vs_install}/VC/Auxiliary/Build/vcvars64.bat" _vcvars)
        file(TO_NATIVE_PATH "${_ossl_inst}" _ossl_inst_native)
        file(TO_NATIVE_PATH "${_ossl_src}" _ossl_src_native)
        file(TO_NATIVE_PATH "${_ossl_perl}" _ossl_perl_native)
        set(_ossl_build_script "${_ossl_root}/build-openssl.cmd")
        file(WRITE "${_ossl_build_script}"
                "@echo on\r\n"
                "call \"${_vcvars}\"\r\n"
                "if errorlevel 1 exit /b %errorlevel%\r\n"
                "cd /d \"${_ossl_src_native}\"\r\n"
                "\"${_ossl_perl_native}\" Configure VC-WIN64A --prefix=\"${_ossl_inst_native}\" --openssldir=\"${_ossl_inst_native}\\ssl\" shared no-tests no-asm\r\n"
                "if errorlevel 1 exit /b %errorlevel%\r\n"
                "nmake\r\n"
                "if errorlevel 1 exit /b %errorlevel%\r\n"
                "nmake install_sw\r\n")
        execute_process(COMMAND cmd /d /c "${_ossl_build_script}" RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[OpenSSL] Windows source build failed (rc=${_rc})")
        endif()
    else()
        include(ProcessorCount)
        ProcessorCount(_jobs)
        if(_jobs LESS 1)
            set(_jobs 1)
        endif()
        execute_process(
                COMMAND ./config --prefix=${_ossl_inst} --openssldir=${_ossl_inst}/ssl shared no-tests
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
    endif()
    file(TOUCH "${_ossl_stamp}")
endif()

set(OPENSSL_ROOT_DIR "${_ossl_inst}" CACHE PATH "OpenSSL root" FORCE)
set(OPENSSL_USE_STATIC_LIBS OFF)
find_package(OpenSSL REQUIRED)
message(STATUS "[OpenSSL] built locally (shared) at ${OPENSSL_ROOT_DIR}")
