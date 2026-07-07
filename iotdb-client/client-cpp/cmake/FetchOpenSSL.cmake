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
# Builds Tongsuo (OpenSSL-compatible, Apache-2.0) from source for Thrift
# TSSLSocket and iotdb_session. Tongsuo adds Chinese commercial cipher / TLCP
# support on top of the standard TLS stack.
#
# Side effects:
#   Sets OPENSSL_ROOT_DIR to the local Tongsuo install tree, then defines
#   imported targets OpenSSL::SSL / OpenSSL::Crypto via find_package so callers
#   can link against them unchanged.
# =============================================================================

# --- Build Tongsuo ${TONGSUO_GIT_REF} from source ---
if(TONGSUO_GIT_REF MATCHES "^[0-9a-fA-F]{7,40}$")
    set(_tongsuo_extracted_dir "Tongsuo-${TONGSUO_GIT_REF}")
    set(_tongsuo_url "https://github.com/Tongsuo-Project/Tongsuo/archive/${TONGSUO_GIT_REF}.tar.gz")
elseif(TONGSUO_GIT_REF MATCHES "^[0-9]+\\.[0-9]")
    set(_tongsuo_extracted_dir "Tongsuo-${TONGSUO_GIT_REF}")
    set(_tongsuo_url
            "https://github.com/Tongsuo-Project/Tongsuo/archive/refs/tags/${TONGSUO_GIT_REF}.tar.gz")
else()
    set(_tongsuo_extracted_dir "Tongsuo-${TONGSUO_GIT_REF}")
    set(_tongsuo_url
            "https://github.com/Tongsuo-Project/Tongsuo/archive/refs/heads/${TONGSUO_GIT_REF}.tar.gz")
endif()

set(_tongsuo_tarname "tongsuo-${TONGSUO_GIT_REF}.tar.gz")
set(_tongsuo_tarball "${IOTDB_OS_DEPS_DIR}/${_tongsuo_tarname}")

if(NOT EXISTS "${_tongsuo_tarball}")
    if(IOTDB_OFFLINE)
        message(FATAL_ERROR
                "[Tongsuo] IOTDB_OFFLINE=ON but ${_tongsuo_tarname} is missing in ${IOTDB_OS_DEPS_DIR}.")
    endif()
    message(STATUS "[Tongsuo] downloading ${_tongsuo_url}")
    file(DOWNLOAD "${_tongsuo_url}" "${_tongsuo_tarball}"
            SHOW_PROGRESS TLS_VERIFY ON
            TIMEOUT 600
            STATUS _st)
    list(GET _st 0 _code)
    if(NOT _code EQUAL 0)
        list(GET _st 1 _msg)
        file(REMOVE "${_tongsuo_tarball}")
        message(FATAL_ERROR "[Tongsuo] download failed: ${_msg}")
    endif()
endif()

if(TONGSUO_TARBALL_SHA256)
    file(SHA256 "${_tongsuo_tarball}" _tongsuo_actual_sha256)
    string(TOLOWER "${TONGSUO_TARBALL_SHA256}" _tongsuo_expected_sha256)
    string(TOLOWER "${_tongsuo_actual_sha256}" _tongsuo_actual_sha256)
    if(NOT _tongsuo_actual_sha256 STREQUAL _tongsuo_expected_sha256)
        file(REMOVE "${_tongsuo_tarball}")
        message(FATAL_ERROR
                "[Tongsuo] tarball SHA256 mismatch for ${_tongsuo_tarname}: "
                "expected ${_tongsuo_expected_sha256}, got ${_tongsuo_actual_sha256}")
    endif()
endif()

set(_tongsuo_root  "${CMAKE_BINARY_DIR}/_deps/tongsuo")
set(_tongsuo_src   "${_tongsuo_root}/src/${_tongsuo_extracted_dir}")
set(_tongsuo_inst  "${_tongsuo_root}/install")
set(_tongsuo_stamp "${_tongsuo_root}/.built-${TONGSUO_GIT_REF}")

if(NOT EXISTS "${_tongsuo_stamp}")
    file(REMOVE_RECURSE "${_tongsuo_root}/src")
    file(MAKE_DIRECTORY "${_tongsuo_root}/src")
    message(STATUS "[Tongsuo] extracting ${_tongsuo_tarball}")
    file(ARCHIVE_EXTRACT INPUT "${_tongsuo_tarball}" DESTINATION "${_tongsuo_root}/src")

    include(ProcessorCount)
    ProcessorCount(_jobs)
    if(_jobs LESS 1)
        set(_jobs 1)
    endif()

    if(WIN32)
        # Git Bash ships a minimal MSYS perl that lacks modules required by
        # Tongsuo/OpenSSL Configure (e.g. Locale::Maketext::Simple). Prefer
        # Strawberry Perl installed by CI (choco) or local dev machines.
        set(_strawberry_perl "C:/Strawberry/perl/bin/perl.exe")
        if(EXISTS "${_strawberry_perl}")
            set(PERL_EXECUTABLE "${_strawberry_perl}")
        else()
            find_program(PERL_EXECUTABLE NAMES perl.exe perl REQUIRED)
        endif()
        message(STATUS "[Tongsuo] using Perl: ${PERL_EXECUTABLE}")
        find_program(NMAKE_EXECUTABLE nmake)
        if(NOT NMAKE_EXECUTABLE AND CMAKE_CXX_COMPILER)
            get_filename_component(_msvc_bin_dir "${CMAKE_CXX_COMPILER}" DIRECTORY)
            find_program(NMAKE_EXECUTABLE nmake PATHS "${_msvc_bin_dir}" NO_DEFAULT_PATH)
        endif()
        if(NOT NMAKE_EXECUTABLE AND DEFINED ENV{VCINSTALLDIR})
            file(GLOB _nmake_candidates "$ENV{VCINSTALLDIR}/Tools/MSVC/*/bin/Hostx64/x64/nmake.exe")
            if(_nmake_candidates)
                list(GET _nmake_candidates 0 NMAKE_EXECUTABLE)
            endif()
        endif()
        if(NOT NMAKE_EXECUTABLE)
            file(GLOB _nmake_candidates
                "C:/Program Files (x86)/Microsoft Visual Studio/2017/*/VC/Tools/MSVC/*/bin/Hostx64/x64/nmake.exe"
                "C:/Program Files/Microsoft Visual Studio/2022/*/VC/Tools/MSVC/*/bin/Hostx64/x64/nmake.exe"
                "C:/Program Files/Microsoft Visual Studio/18/*/VC/Tools/MSVC/*/bin/Hostx64/x64/nmake.exe")
            if(_nmake_candidates)
                list(SORT _nmake_candidates COMPARE NATURAL ORDER DESCENDING)
                list(GET _nmake_candidates 0 NMAKE_EXECUTABLE)
            endif()
        endif()
        if(NOT NMAKE_EXECUTABLE)
            message(FATAL_ERROR "[Tongsuo] nmake not found (install VS Build Tools or run from Developer Command Prompt)")
        endif()
        message(STATUS "[Tongsuo] using nmake: ${NMAKE_EXECUTABLE}")
        set(_vcvars "")
        if(CMAKE_CXX_COMPILER)
            get_filename_component(_cl_exe "${CMAKE_CXX_COMPILER}" REALPATH)
            set(_vc_dir "${_cl_exe}")
            foreach(_unused RANGE 6)
                get_filename_component(_vc_dir "${_vc_dir}" DIRECTORY)
            endforeach()
            set(_vcvars "${_vc_dir}/Auxiliary/Build/vcvars64.bat")
        elseif(DEFINED ENV{VCINSTALLDIR})
            set(_vcvars "$ENV{VCINSTALLDIR}/Auxiliary/Build/vcvars64.bat")
        else()
            get_filename_component(_nmake_dir "${NMAKE_EXECUTABLE}" DIRECTORY)
            set(_vc_dir "${_nmake_dir}")
            foreach(_unused RANGE 6)
                get_filename_component(_vc_dir "${_vc_dir}" DIRECTORY)
            endforeach()
            set(_vcvars "${_vc_dir}/Auxiliary/Build/vcvars64.bat")
        endif()
        if(NOT EXISTS "${_vcvars}")
            message(FATAL_ERROR "[Tongsuo] vcvars64.bat not found (CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})")
        endif()
        file(TO_NATIVE_PATH "${_vcvars}" _vcvars_native)
        file(TO_NATIVE_PATH "${NMAKE_EXECUTABLE}" _nmake_native)
        file(TO_NATIVE_PATH "${_tongsuo_src}" _tongsuo_src_native)
        set(_nmake_build_bat "${_tongsuo_root}/tongsuo-nmake-build.bat")
        set(_nmake_install_bat "${_tongsuo_root}/tongsuo-nmake-install.bat")
        file(WRITE "${_nmake_build_bat}" "@echo off\r\n")
        file(APPEND "${_nmake_build_bat}" "call \"${_vcvars_native}\" amd64\r\n")
        file(APPEND "${_nmake_build_bat}" "if errorlevel 1 exit /b 1\r\n")
        file(APPEND "${_nmake_build_bat}" "cd /d \"${_tongsuo_src_native}\"\r\n")
        file(APPEND "${_nmake_build_bat}" "\"${_nmake_native}\"\r\n")
        file(APPEND "${_nmake_build_bat}" "exit /b %ERRORLEVEL%\r\n")
        file(WRITE "${_nmake_install_bat}" "@echo off\r\n")
        file(APPEND "${_nmake_install_bat}" "call \"${_vcvars_native}\" amd64\r\n")
        file(APPEND "${_nmake_install_bat}" "if errorlevel 1 exit /b 1\r\n")
        file(APPEND "${_nmake_install_bat}" "cd /d \"${_tongsuo_src_native}\"\r\n")
        file(APPEND "${_nmake_install_bat}" "\"${_nmake_native}\" install_sw\r\n")
        file(APPEND "${_nmake_install_bat}" "exit /b %ERRORLEVEL%\r\n")
        set(_tongsuo_target "VC-WIN64A")
        message(STATUS "[Tongsuo] configuring (${_tongsuo_target}) -> ${_tongsuo_inst}")
        execute_process(
                COMMAND "${PERL_EXECUTABLE}" Configure enable-ntls no-asm ${_tongsuo_target}
                        --prefix=${_tongsuo_inst}
                        --openssldir=${_tongsuo_inst}/ssl
                WORKING_DIRECTORY "${_tongsuo_src}"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[Tongsuo] Configure failed (rc=${_rc})")
        endif()

        message(STATUS "[Tongsuo] building")
        execute_process(
                COMMAND "${_nmake_build_bat}"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[Tongsuo] nmake failed (rc=${_rc})")
        endif()

        execute_process(
                COMMAND "${_nmake_install_bat}"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[Tongsuo] nmake install_sw failed (rc=${_rc})")
        endif()
    else()
        find_program(PERL_EXECUTABLE NAMES perl REQUIRED)
        message(STATUS "[Tongsuo] using Perl: ${PERL_EXECUTABLE}")
        set(_tongsuo_config_args
                --prefix=${_tongsuo_inst}
                --openssldir=${_tongsuo_inst}/ssl
                shared
                enable-ntls)
        # Assembly optimizations often fail on macOS CI toolchains; match the
        # Windows VC-WIN64A build which already passes no-asm.
        if(APPLE)
            list(APPEND _tongsuo_config_args no-asm)
        endif()
        message(STATUS "[Tongsuo] configuring -> ${_tongsuo_inst}")
        execute_process(
                COMMAND "${PERL_EXECUTABLE}" ./config ${_tongsuo_config_args}
                WORKING_DIRECTORY "${_tongsuo_src}"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[Tongsuo] config failed (rc=${_rc})")
        endif()

        message(STATUS "[Tongsuo] building (-j${_jobs})")
        execute_process(
                COMMAND make -j${_jobs}
                WORKING_DIRECTORY "${_tongsuo_src}"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[Tongsuo] make failed (rc=${_rc})")
        endif()

        execute_process(
                COMMAND make install_sw
                WORKING_DIRECTORY "${_tongsuo_src}"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[Tongsuo] make install_sw failed (rc=${_rc})")
        endif()
    endif()
    file(TOUCH "${_tongsuo_stamp}")
endif()

set(OPENSSL_ROOT_DIR "${_tongsuo_inst}" CACHE PATH "Tongsuo install root" FORCE)
set(OPENSSL_INCLUDE_DIR "${_tongsuo_inst}/include" CACHE PATH "Tongsuo headers" FORCE)
set(OPENSSL_USE_STATIC_LIBS OFF)

if(WIN32)
    # MSVC needs FindOpenSSL imported targets (IMPORTED_IMPLIB + DLL). Hand-rolled
    # SHARED IMPORTED targets break the link line (LNK1104: OpenSSL::SSL-NOTFOUND.obj).
    find_package(OpenSSL REQUIRED)
elseif(APPLE)
    # macOS CI runners ship Homebrew/Xcode OpenSSL headers on the default include
    # path; find_package would satisfy version checks but still compile against the
    # wrong headers. Link against the bundled Tongsuo libs and route <openssl/*.h>
    # through generated wrapper headers (see TongsuoOpenSslHeaders.cmake).
    find_library(_iotdb_tongsuo_ssl NAMES ssl libssl
            PATHS "${_tongsuo_inst}/lib" "${_tongsuo_inst}/lib64"
            NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
    find_library(_iotdb_tongsuo_crypto NAMES crypto libcrypto
            PATHS "${_tongsuo_inst}/lib" "${_tongsuo_inst}/lib64"
            NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
    if(NOT _iotdb_tongsuo_ssl OR NOT _iotdb_tongsuo_crypto)
        message(FATAL_ERROR
                "[Tongsuo] libssl/libcrypto not found under ${_tongsuo_inst}/lib")
    endif()

    if(NOT TARGET OpenSSL::Crypto)
        add_library(OpenSSL::Crypto SHARED IMPORTED)
    endif()
    set_target_properties(OpenSSL::Crypto PROPERTIES
            IMPORTED_LOCATION "${_iotdb_tongsuo_crypto}"
            INTERFACE_INCLUDE_DIRECTORIES "${_tongsuo_inst}/include")

    if(NOT TARGET OpenSSL::SSL)
        add_library(OpenSSL::SSL SHARED IMPORTED)
    endif()
    set_target_properties(OpenSSL::SSL PROPERTIES
            IMPORTED_LOCATION "${_iotdb_tongsuo_ssl}"
            INTERFACE_INCLUDE_DIRECTORIES "${_tongsuo_inst}/include"
            INTERFACE_LINK_LIBRARIES OpenSSL::Crypto)

    set(OPENSSL_SSL_LIBRARY "${_iotdb_tongsuo_ssl}" CACHE FILEPATH "" FORCE)
    set(OPENSSL_CRYPTO_LIBRARY "${_iotdb_tongsuo_crypto}" CACHE FILEPATH "" FORCE)
    set(OPENSSL_VERSION_MAJOR 3 CACHE STRING "" FORCE)

    include(TongsuoOpenSslHeaders)
    iotdb_setup_tongsuo_openssl_headers("${_tongsuo_inst}/include")
else()
    find_package(OpenSSL REQUIRED)
endif()

message(STATUS "[Tongsuo] built from source (shared) at ${OPENSSL_ROOT_DIR}")
message(STATUS "[Tongsuo] OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}")
