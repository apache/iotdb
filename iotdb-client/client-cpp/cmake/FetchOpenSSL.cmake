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
# Resolves the selected NTLS provider. Tongsuo is built from source for
# Thrift TSSLSocket; GmSSL uses a preinstalled native TLCP library.
#
# Side effects:
#   TONGSUO defines OpenSSL::SSL / OpenSSL::Crypto; GMSSL defines IoTDB::gmssl.
#   IOTDB_NTLS_RUNTIME_LIBRARIES lists the selected provider's runtime files.
# =============================================================================

# --- Default provider: build Tongsuo ${TONGSUO_GIT_REF} from source ---
if(IOTDB_NTLS_PROVIDER STREQUAL "TONGSUO")
string(LENGTH "${TONGSUO_GIT_REF}" _tongsuo_git_ref_length)
if(TONGSUO_GIT_REF MATCHES "^[0-9a-fA-F]+$"
        AND _tongsuo_git_ref_length GREATER_EQUAL 7
        AND _tongsuo_git_ref_length LESS_EQUAL 40)
    set(_tongsuo_extracted_dir "Tongsuo-${TONGSUO_GIT_REF}")
    set(_tongsuo_url "https://github.com/Tongsuo-Project/Tongsuo/archive/${TONGSUO_GIT_REF}.tar.gz")
else()
    set(_tongsuo_extracted_dir "Tongsuo-${TONGSUO_GIT_REF}")
    set(_tongsuo_url
            "https://github.com/Tongsuo-Project/Tongsuo/archive/refs/heads/${TONGSUO_GIT_REF}.tar.gz")
endif()

set(_tongsuo_tarname "tongsuo-${TONGSUO_GIT_REF}.tar.gz")
set(_tongsuo_tarball "${IOTDB_OS_DEPS_DIR}/${_tongsuo_tarname}")

string(LENGTH "${TONGSUO_SHA256}" _tongsuo_sha256_length)
if(NOT TONGSUO_SHA256 MATCHES "^[0-9a-fA-F]+$" OR NOT _tongsuo_sha256_length EQUAL 64)
    message(FATAL_ERROR
            "[Tongsuo] TONGSUO_SHA256 must be the 64-character SHA-256 of ${_tongsuo_tarname}")
endif()

if(EXISTS "${_tongsuo_tarball}")
    file(SHA256 "${_tongsuo_tarball}" _tongsuo_cached_sha256)
    if(NOT "${_tongsuo_cached_sha256}" STREQUAL "${TONGSUO_SHA256}")
        if(IOTDB_OFFLINE)
            message(FATAL_ERROR
                    "[Tongsuo] cached ${_tongsuo_tarname} has SHA-256 ${_tongsuo_cached_sha256}; "
                    "expected ${TONGSUO_SHA256}")
        endif()
        message(STATUS "[Tongsuo] removing cached archive with mismatched SHA-256")
        file(REMOVE "${_tongsuo_tarball}")
    endif()
endif()

if(NOT EXISTS "${_tongsuo_tarball}")
    if(IOTDB_OFFLINE)
        message(FATAL_ERROR
                "[Tongsuo] IOTDB_OFFLINE=ON but ${_tongsuo_tarname} is missing in ${IOTDB_OS_DEPS_DIR}.")
    endif()
    message(STATUS "[Tongsuo] downloading ${_tongsuo_url}")
    file(DOWNLOAD "${_tongsuo_url}" "${_tongsuo_tarball}"
            SHOW_PROGRESS TLS_VERIFY ON
            TIMEOUT 600
            EXPECTED_HASH "SHA256=${TONGSUO_SHA256}"
            STATUS _st)
    list(GET _st 0 _code)
    if(NOT _code EQUAL 0)
        list(GET _st 1 _msg)
        file(REMOVE "${_tongsuo_tarball}")
        message(FATAL_ERROR "[Tongsuo] download failed: ${_msg}")
    endif()
endif()

set(_tongsuo_root  "${CMAKE_BINARY_DIR}/_deps/tongsuo")
set(_tongsuo_src   "${_tongsuo_root}/src/${_tongsuo_extracted_dir}")
set(_tongsuo_inst  "${_tongsuo_root}/install")
set(_tongsuo_stamp "${_tongsuo_root}/.built-${TONGSUO_GIT_REF}-${TONGSUO_SHA256}")

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
                COMMAND "${CMAKE_COMMAND}" -E env "CC=cl" "CXX=cl"
                        "${PERL_EXECUTABLE}" Configure enable-ntls no-asm ${_tongsuo_target}
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
        message(STATUS "[Tongsuo] configuring -> ${_tongsuo_inst}")
        execute_process(
                COMMAND ./config --prefix=${_tongsuo_inst} --openssldir=${_tongsuo_inst}/ssl shared enable-ntls
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
set(OPENSSL_USE_STATIC_LIBS OFF)
# Do not reuse paths cached by an earlier configure that resolved the system
# OpenSSL. WITH_SSL requires Tongsuo because RpcSslUtils uses its TLCP APIs.
#
# OPENSSL_ROOT_DIR alone is not sufficient on macOS: Homebrew's /usr/local/include
# can still win FindOpenSSL's header search even while the libraries are resolved
# from OPENSSL_ROOT_DIR. That produces an unusable system-header/Tongsuo-library
# combination, so pin the headers to the Tongsuo installation as well.
set(OPENSSL_INCLUDE_DIR "${_tongsuo_inst}/include"
        CACHE PATH "Tongsuo include directory" FORCE)
unset(OPENSSL_SSL_LIBRARY CACHE)
unset(OPENSSL_CRYPTO_LIBRARY CACHE)
find_package(OpenSSL REQUIRED)
get_filename_component(_tongsuo_expected_include "${_tongsuo_inst}/include" REALPATH)
get_filename_component(_tongsuo_resolved_include "${OPENSSL_INCLUDE_DIR}" REALPATH)
if(NOT _tongsuo_resolved_include STREQUAL _tongsuo_expected_include)
    message(FATAL_ERROR
            "[Tongsuo] FindOpenSSL selected headers from ${OPENSSL_INCLUDE_DIR}; "
            "expected ${_tongsuo_inst}/include")
endif()
set(IOTDB_NTLS_RUNTIME_LIBRARIES
        "${OPENSSL_SSL_LIBRARY};${OPENSSL_CRYPTO_LIBRARY}"
        CACHE INTERNAL "NTLS provider runtime libraries" FORCE)
message(STATUS "[Tongsuo] built from source (shared) at ${OPENSSL_ROOT_DIR}")

# --- Alternative provider: use preinstalled GmSSL 3 through its native TLCP API ---
elseif(IOTDB_NTLS_PROVIDER STREQUAL "GMSSL")
    if(NOT IOTDB_GMSSL_ROOT_DIR OR NOT IS_DIRECTORY "${IOTDB_GMSSL_ROOT_DIR}")
        message(FATAL_ERROR
                "[GmSSL] IOTDB_GMSSL_ROOT_DIR must point to a preinstalled GmSSL 3.2")
    endif()

    unset(_gmssl_include_dir CACHE)
    unset(_gmssl_library CACHE)
    find_path(_gmssl_include_dir gmssl/tls.h
            PATHS "${IOTDB_GMSSL_ROOT_DIR}/include" NO_DEFAULT_PATH REQUIRED)
    find_library(_gmssl_library NAMES gmssl libgmssl
            PATHS "${IOTDB_GMSSL_ROOT_DIR}/lib" "${IOTDB_GMSSL_ROOT_DIR}/lib64"
            NO_DEFAULT_PATH REQUIRED)

    include(CMakePushCheckState)
    include(CheckCXXSourceCompiles)
    include(CheckCXXSourceRuns)
    cmake_push_check_state(RESET)
    set(CMAKE_REQUIRED_INCLUDES "${_gmssl_include_dir}")
    set(CMAKE_REQUIRED_LIBRARIES "${_gmssl_library}")
    if(WIN32)
        list(APPEND CMAKE_REQUIRED_LIBRARIES ws2_32)
        set(_gmssl_saved_path "$ENV{PATH}")
        set(ENV{PATH} "${IOTDB_GMSSL_ROOT_DIR}/bin;$ENV{PATH}")
    else()
        set(_gmssl_saved_library_path "$ENV{LD_LIBRARY_PATH}")
        set(ENV{LD_LIBRARY_PATH}
                "${IOTDB_GMSSL_ROOT_DIR}/lib:${IOTDB_GMSSL_ROOT_DIR}/lib64:$ENV{LD_LIBRARY_PATH}")
    endif()

    set(_gmssl_compile_definitions "")
    macro(_iotdb_probe_gmssl_abi_definition _definition _symbol)
        string(MAKE_C_IDENTIFIER
                "IOTDB_GMSSL_HAS_${_definition}_${_symbol}" _probe_variable)
        unset(${_probe_variable} CACHE)
        check_cxx_source_compiles(
                "extern \"C\" void ${_symbol}();\nint main() { ${_symbol}(); return 0; }"
                ${_probe_variable})
        if(${_probe_variable})
            list(APPEND _gmssl_compile_definitions "${_definition}")
        endif()
    endmacro()
    _iotdb_probe_gmssl_abi_definition(ENABLE_SHA1 sha1_init)
    _iotdb_probe_gmssl_abi_definition(ENABLE_SHA2 sha256_init)
    _iotdb_probe_gmssl_abi_definition(ENABLE_AES aes_set_encrypt_key)
    _iotdb_probe_gmssl_abi_definition(ENABLE_SECP256R1 x509_key_set_secp256r1_key)
    _iotdb_probe_gmssl_abi_definition(ENABLE_LMS x509_key_set_lms_key)
    _iotdb_probe_gmssl_abi_definition(ENABLE_XMSS x509_key_set_xmss_key)
    _iotdb_probe_gmssl_abi_definition(ENABLE_SPHINCS x509_key_set_sphincs_key)
    _iotdb_probe_gmssl_abi_definition(ENABLE_KYBER x509_key_set_kyber_key)
    _iotdb_probe_gmssl_abi_definition(ENABLE_SM9 x509_key_set_sm9_sign_key)
    unset(_iotdb_probe_gmssl_abi_definition)
    message(STATUS "[GmSSL] detected ABI definitions: ${_gmssl_compile_definitions}")

    foreach(_definition IN LISTS _gmssl_compile_definitions)
        list(APPEND CMAKE_REQUIRED_DEFINITIONS "-D${_definition}")
    endforeach()
    unset(IOTDB_GMSSL_ABI_COMPATIBLE CACHE)
    check_cxx_source_runs([=[
        #include <gmssl/tls.h>
        #include <gmssl/version.h>
        #include <cstdint>
        #include <cstring>
        #if GMSSL_VERSION_NUM < 30200 || GMSSL_VERSION_NUM >= 30300
        #error "IoTDB requires GmSSL 3.2.x"
        #endif
        struct GuardedContext {
          TLS_CTX context;
          std::uint64_t canary[8];
        };
        int main() {
          GuardedContext guarded{};
          std::memset(guarded.canary, 0xA5, sizeof(guarded.canary));
          if (tls_ctx_init(&guarded.context, TLS_protocol_tlcp, 1) != 1) {
            return 1;
          }
          const int cipher = TLS_cipher_ecc_sm4_cbc_sm3;
          if (tls_ctx_set_cipher_suites(&guarded.context, &cipher, 1) != 1 ||
              guarded.context.is_client != 1 ||
              guarded.context.protocol != TLS_protocol_tlcp ||
              guarded.context.cipher_suites_cnt != 1 ||
              guarded.context.cipher_suites[0] != cipher) {
            tls_ctx_cleanup(&guarded.context);
            return 2;
          }
          const std::uint64_t expected = UINT64_C(0xA5A5A5A5A5A5A5A5);
          for (std::uint64_t value : guarded.canary) {
            if (value != expected) {
              tls_ctx_cleanup(&guarded.context);
              return 3;
            }
          }
          tls_ctx_cleanup(&guarded.context);
          return 0;
        }
    ]=] IOTDB_GMSSL_ABI_COMPATIBLE)
    if(WIN32)
        set(ENV{PATH} "${_gmssl_saved_path}")
    else()
        set(ENV{LD_LIBRARY_PATH} "${_gmssl_saved_library_path}")
    endif()
    cmake_pop_check_state()
    if(NOT IOTDB_GMSSL_ABI_COMPATIBLE)
        message(FATAL_ERROR
                "[GmSSL] headers/library ABI check failed after probing its "
                "ABI-affecting ENABLE_* symbols.")
    endif()

    if(NOT TARGET IoTDB::gmssl)
        add_library(IoTDB::gmssl UNKNOWN IMPORTED GLOBAL)
        set_target_properties(IoTDB::gmssl PROPERTIES
                IMPORTED_LOCATION "${_gmssl_library}"
                INTERFACE_INCLUDE_DIRECTORIES "${_gmssl_include_dir}"
                INTERFACE_COMPILE_DEFINITIONS "${_gmssl_compile_definitions}")
    endif()

    set(IOTDB_NTLS_RUNTIME_LIBRARIES
            "${_gmssl_library}"
            CACHE INTERNAL "NTLS provider runtime libraries" FORCE)
    message(STATUS "[GmSSL] using native GmSSL TLCP library ${_gmssl_library}")
endif()
