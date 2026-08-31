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
# FetchThrift.cmake
#
# Downloads (or uses a local copy of) the Apache Thrift source tarball and
# builds it from source as a static-only, runtime-and-compiler artifact. The
# build runs at configure time so the thrift compiler is available for the
# code generation step that follows.
#
# Exported variables:
#   THRIFT_EXECUTABLE     - path to the thrift binary that just got built
#   THRIFT_INCLUDE_DIR    - include directory containing <thrift/Thrift.h>
#   THRIFT_STATIC_LIB_PATH- path to libthrift.a / thriftmd.lib
#   THRIFT_RUNTIME_LIB_DIR- directory containing the static thrift library
#                           (and any other build artifacts you may want to
#                           bundle with the SDK)
#
# Imported targets:
#   iotdb_thrift_static   - INTERFACE-compatible IMPORTED target that
#                           propagates include dirs and the static lib.
#   iotdb_thrift_external - phony custom target representing the build
#                           (use add_dependencies(<your-target> iotdb_thrift_external)
#                           to ensure the thrift build runs first).
# =============================================================================

include(ExternalProject)

set(_thrift_dirname "thrift-${THRIFT_VERSION}")
set(_thrift_tarname "${_thrift_dirname}.tar.gz")

# ---------------------------------------------------------------------------
# Resolve tarball (local cache -> download)
# ---------------------------------------------------------------------------
set(_thrift_tarball "${IOTDB_OS_DEPS_DIR}/${_thrift_tarname}")
if(NOT EXISTS "${_thrift_tarball}")
    if(IOTDB_OFFLINE)
        message(FATAL_ERROR
                "[Thrift] IOTDB_OFFLINE=ON but ${_thrift_tarname} is missing in "
                "${IOTDB_OS_DEPS_DIR}.")
    endif()
    set(_thrift_url "https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/${_thrift_tarname}")
    message(STATUS "[Thrift] downloading ${_thrift_url}")
    file(DOWNLOAD "${_thrift_url}" "${_thrift_tarball}"
            SHOW_PROGRESS TLS_VERIFY ON STATUS _thrift_dl)
    list(GET _thrift_dl 0 _code)
    if(NOT _code EQUAL 0)
        list(GET _thrift_dl 1 _msg)
        file(REMOVE "${_thrift_tarball}")
        message(FATAL_ERROR "[Thrift] download failed: ${_msg}")
    endif()
endif()

# ---------------------------------------------------------------------------
# Extract once into ${CMAKE_BINARY_DIR}/_deps/thrift/src
# ---------------------------------------------------------------------------
set(_thrift_root  "${CMAKE_BINARY_DIR}/_deps/thrift")
set(_thrift_src   "${_thrift_root}/src/${_thrift_dirname}")
set(_thrift_build "${_thrift_root}/build")
set(_thrift_install "${_thrift_root}/install")
set(_thrift_marker "${_thrift_root}/.extracted-${THRIFT_VERSION}")

set(_thrift_build_config "Release")
if(MSVC AND CMAKE_BUILD_TYPE)
    set(_thrift_build_config "${CMAKE_BUILD_TYPE}")
endif()

if(NOT EXISTS "${_thrift_marker}")
    file(REMOVE_RECURSE "${_thrift_root}/src")
    file(MAKE_DIRECTORY "${_thrift_root}/src")
    message(STATUS "[Thrift] extracting ${_thrift_tarball}")
    file(ARCHIVE_EXTRACT INPUT "${_thrift_tarball}"
            DESTINATION "${_thrift_root}/src")
    file(TOUCH "${_thrift_marker}")
endif()

if(NOT EXISTS "${_thrift_src}/CMakeLists.txt")
    message(FATAL_ERROR
            "[Thrift] could not find ${_thrift_src}/CMakeLists.txt after "
            "extracting ${_thrift_tarball}.")
endif()

# ---------------------------------------------------------------------------
# ExternalProject_Add: build thrift at *configure* time so the produced
# binary / library can immediately drive code generation and linking.
# ---------------------------------------------------------------------------
set(_thrift_cmake_args
        # CMake 4.x rejects Thrift's old cmake_minimum_required(3.x); set policy first.
        "-DCMAKE_POLICY_VERSION_MINIMUM=3.5"
        "-DCMAKE_INSTALL_PREFIX=${_thrift_install}"
        "-DCMAKE_BUILD_TYPE=${_thrift_build_config}"
        "-DBUILD_JAVA=OFF"
        "-DBUILD_NODEJS=OFF"
        "-DBUILD_JAVASCRIPT=OFF"
        "-DBUILD_PYTHON=OFF"
        "-DBUILD_TESTING=OFF"
        "-DBUILD_TUTORIALS=OFF"
        "-DBUILD_SHARED_LIBS=OFF"
        "-DWITH_SHARED_LIB=OFF"
        "-DWITH_STATIC_LIB=ON"
        "-DCMAKE_POSITION_INDEPENDENT_CODE=ON"
        "-DCMAKE_POLICY_DEFAULT_CMP0091=NEW"
        "-DCMAKE_CXX_STANDARD=11")

if(BOOST_INCLUDE_DIR)
    list(APPEND _thrift_cmake_args
            "-DBoost_INCLUDE_DIR=${BOOST_INCLUDE_DIR}"
            "-DBOOST_INCLUDEDIR=${BOOST_INCLUDE_DIR}")
endif()

if(MSVC)
    list(APPEND _thrift_cmake_args
            "-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
else()
    set(_thrift_cxxflags "-fPIC")
    if(IOTDB_USE_CXX11_ABI)
        set(_thrift_cxxflags "${_thrift_cxxflags} -D_GLIBCXX_USE_CXX11_ABI=${IOTDB_USE_CXX11_ABI}")
    endif()
    list(APPEND _thrift_cmake_args
            "-DCMAKE_C_FLAGS=-fPIC"
            "-DCMAKE_CXX_FLAGS=${_thrift_cxxflags}")
endif()

if(WITH_SSL)
    list(APPEND _thrift_cmake_args "-DWITH_OPENSSL=ON")
    # Build Thrift's TSSLSocket against the same OpenSSL that iotdb_session links
    # and bundles, so the runtime libraries match. find_package does not set
    # OPENSSL_ROOT_DIR itself, so derive it from the resolved include dir.
    if(OPENSSL_ROOT_DIR)
        list(APPEND _thrift_cmake_args "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}")
    elseif(OPENSSL_INCLUDE_DIR)
        get_filename_component(_thrift_ossl_root "${OPENSSL_INCLUDE_DIR}" DIRECTORY)
        list(APPEND _thrift_cmake_args "-DOPENSSL_ROOT_DIR=${_thrift_ossl_root}")
    endif()
else()
    list(APPEND _thrift_cmake_args "-DWITH_OPENSSL=OFF")
endif()

# Run the ExternalProject build immediately so the thrift compiler is
# available for the subsequent code-generation step. We do this by
# invoking cmake twice via execute_process and only register a phony
# ExternalProject for dependency ordering.

if(IOTDB_USE_CXX11_ABI)
    set(_thrift_abi_stamp "-abi${IOTDB_USE_CXX11_ABI}")
else()
    set(_thrift_abi_stamp "-abidefault")
endif()
# Encode WITH_SSL in the stamp: toggling SSL changes WITH_OPENSSL, so a cached
# build of the opposite flavour must not be reused (otherwise TSSLSocket is
# missing/extra at link time).
if(WITH_SSL)
    set(_thrift_ssl_stamp "-ssl")
else()
    set(_thrift_ssl_stamp "-nossl")
endif()
set(_thrift_stamp "${_thrift_build}/.built-${THRIFT_VERSION}-${_thrift_build_config}-mdll${_thrift_abi_stamp}${_thrift_ssl_stamp}")
if(NOT EXISTS "${_thrift_stamp}")
    file(MAKE_DIRECTORY "${_thrift_build}")
    message(STATUS "[Thrift] configuring ${_thrift_dirname}")

    # When we built m4/flex/bison locally, make sure CMake passes the
    # updated PATH down to the nested cmake invocation.
    if(IOTDB_LOCAL_TOOLS_BIN)
        set(_thrift_env "PATH=${IOTDB_LOCAL_TOOLS_BIN}:$ENV{PATH}")
    endif()

    if(CMAKE_GENERATOR_PLATFORM)
        set(_gen_platform_arg "-A" "${CMAKE_GENERATOR_PLATFORM}")
    else()
        set(_gen_platform_arg "")
    endif()

    execute_process(
            COMMAND ${CMAKE_COMMAND}
                    -G "${CMAKE_GENERATOR}"
                    ${_gen_platform_arg}
                    ${_thrift_cmake_args}
                    "${_thrift_src}"
            WORKING_DIRECTORY "${_thrift_build}"
            RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[Thrift] configure step failed (rc=${_rc})")
    endif()

    message(STATUS "[Thrift] building (${_thrift_build_config})")
    execute_process(
            COMMAND ${CMAKE_COMMAND} --build . --config ${_thrift_build_config} --target install
            WORKING_DIRECTORY "${_thrift_build}"
            RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[Thrift] build/install step failed (rc=${_rc})")
    endif()
    file(TOUCH "${_thrift_stamp}")
endif()

# ---------------------------------------------------------------------------
# Locate produced artifacts
# ---------------------------------------------------------------------------
set(THRIFT_INCLUDE_DIR "${_thrift_install}/include" CACHE PATH "" FORCE)

# Thrift binary
if(WIN32)
    find_program(THRIFT_EXECUTABLE
            NAMES thrift
            HINTS "${_thrift_install}/bin" "${_thrift_build}/bin/Release"
                  "${_thrift_build}/compiler/cpp/Release"
                  "${_thrift_build}/compiler/cpp/bin/Release"
            NO_DEFAULT_PATH)
else()
    find_program(THRIFT_EXECUTABLE
            NAMES thrift
            HINTS "${_thrift_install}/bin"
                  "${_thrift_build}/bin"
                  "${_thrift_build}/compiler/cpp/bin"
                  "${_thrift_build}/compiler/cpp"
            NO_DEFAULT_PATH)
endif()

if(NOT THRIFT_EXECUTABLE)
    message(FATAL_ERROR
            "[Thrift] could not find the thrift binary under ${_thrift_install} or ${_thrift_build}")
endif()
message(STATUS "[Thrift] THRIFT_EXECUTABLE = ${THRIFT_EXECUTABLE}")

# Thrift static library (search a few standard install/lib locations).
set(_thrift_libname_candidates)
if(MSVC)
    if(_thrift_build_config STREQUAL "Debug")
        list(APPEND _thrift_libname_candidates thriftmdd.lib thriftmd.lib thriftmtd.lib thriftmt.lib thrift.lib)
    else()
        list(APPEND _thrift_libname_candidates thriftmd.lib thriftmt.lib thrift.lib)
    endif()
else()
    list(APPEND _thrift_libname_candidates libthrift.a)
endif()

set(THRIFT_STATIC_LIB_PATH "")
foreach(_dir
        "${_thrift_install}/lib"
        "${_thrift_install}/lib64"
        "${_thrift_build}/lib"
        "${_thrift_build}/lib/Release"
        "${_thrift_build}/lib/release")
    if(THRIFT_STATIC_LIB_PATH)
        break()
    endif()
    foreach(_n ${_thrift_libname_candidates})
        if(EXISTS "${_dir}/${_n}")
            set(THRIFT_STATIC_LIB_PATH "${_dir}/${_n}")
            set(THRIFT_RUNTIME_LIB_DIR "${_dir}")
            break()
        endif()
    endforeach()
endforeach()

if(NOT THRIFT_STATIC_LIB_PATH)
    message(FATAL_ERROR
            "[Thrift] could not locate the thrift static library under ${_thrift_install}/lib")
endif()
message(STATUS "[Thrift] THRIFT_STATIC_LIB_PATH = ${THRIFT_STATIC_LIB_PATH}")

# Cache as well so subsequent reconfigures keep the same values.
set(THRIFT_STATIC_LIB_PATH "${THRIFT_STATIC_LIB_PATH}" CACHE FILEPATH "" FORCE)
set(THRIFT_RUNTIME_LIB_DIR "${THRIFT_RUNTIME_LIB_DIR}" CACHE PATH "" FORCE)

# ---------------------------------------------------------------------------
# Imported target wrapping the static library.
# ---------------------------------------------------------------------------
if(NOT TARGET iotdb_thrift_static)
    add_library(iotdb_thrift_static STATIC IMPORTED GLOBAL)
    set_target_properties(iotdb_thrift_static PROPERTIES
            IMPORTED_LOCATION "${THRIFT_STATIC_LIB_PATH}"
            INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_INCLUDE_DIR}")
endif()

# Phony target so downstream code can express ordering deps.
if(NOT TARGET iotdb_thrift_external)
    add_custom_target(iotdb_thrift_external ALL)
endif()
