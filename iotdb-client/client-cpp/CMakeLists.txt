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
# Apache IoTDB - C++ Session Client (top-level CMake build)
# =============================================================================
# This is the single entry point that fully manages the C++ client build,
# including third-party dependencies (Boost headers, Apache Thrift, optional
# OpenSSL) and toolchain bootstrap (m4 / flex / bison on Linux & macOS when
# the system PATH does not provide them).
#
# Standalone usage (no Maven required):
#     cmake -S iotdb-client/client-cpp -B build [options]
#     cmake --build build --config Release --target install
#
# Common options:
#     -DWITH_SSL=ON|OFF           Enable OpenSSL support (default OFF)
#     -DBUILD_TESTING=ON|OFF      Build IT executables (default OFF)
#     -DIOTDB_OFFLINE=ON|OFF      Disable any network access (default OFF).
#                                 Requires tarballs under third-party/{os}/.
#     -DIOTDB_DEPS_DIR=<path>     Override the third-party cache root.
#                                 Default: <client-cpp>/third-party
#                                 (portable: copy the whole IoTDB tree to an
#                                 offline machine after staging tarballs here).
#     -DBOOST_ROOT=<path>         Existing Boost installation to reuse.
#     -DCMAKE_INSTALL_PREFIX=...  Install location (defaults to build/install).
# =============================================================================

cmake_minimum_required(VERSION 3.16)
project(iotdb_session CXX C)

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_POLICY_DEFAULT_CMP0091 NEW)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

if(NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O2")
endif()

if(MSVC)
    # Match thrift's MultiThreaded runtime so static libraries link cleanly.
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

# ---------------------------------------------------------------------------
# Cached options
# ---------------------------------------------------------------------------
option(WITH_SSL "Build with OpenSSL support" OFF)
option(BUILD_TESTING "Build IT test executables" OFF)
option(IOTDB_OFFLINE "Disable all network access during configure" OFF)

set(IOTDB_DEPS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    CACHE PATH "Local tarball cache for third-party dependencies (lives under client-cpp/)")
set(BOOST_VERSION "1.60.0"
    CACHE STRING "Boost version used when downloading / unpacking")
set(THRIFT_VERSION "0.21.0"
    CACHE STRING "Apache Thrift version used when downloading / building")

# ---------------------------------------------------------------------------
# Platform -> local tarball directory
# ---------------------------------------------------------------------------
if(WIN32)
    set(IOTDB_OS_DEPS_DIR "${IOTDB_DEPS_DIR}/windows")
elseif(APPLE)
    set(IOTDB_OS_DEPS_DIR "${IOTDB_DEPS_DIR}/mac")
else()
    set(IOTDB_OS_DEPS_DIR "${IOTDB_DEPS_DIR}/linux")
endif()
file(MAKE_DIRECTORY "${IOTDB_OS_DEPS_DIR}")

# Default install prefix lives inside the build tree so Maven's assembly
# step can pick everything up under a stable path.
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/install"
        CACHE PATH "Install prefix" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# ---------------------------------------------------------------------------
# Fetch / build third-party dependencies
# ---------------------------------------------------------------------------
include(FetchBoost)        # -> BOOST_INCLUDE_DIR
include(FetchBuildTools)   # -> populates ${CMAKE_BINARY_DIR}/tools/bin and updates ENV{PATH}
if(WITH_SSL)
    include(FetchOpenSSL)
endif()
include(FetchThrift)       # -> target iotdb_thrift_static, THRIFT_EXECUTABLE, THRIFT_INCLUDE_DIR
include(GenerateThriftSources)  # -> THRIFT_GENERATED_SRCS, THRIFT_GENERATED_HDRS

# ---------------------------------------------------------------------------
# iotdb_session library
# ---------------------------------------------------------------------------
file(GLOB SESSION_HANDWRITTEN_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/main/*.cpp")
file(GLOB SESSION_HANDWRITTEN_HDRS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/main/*.h")

if(MSVC)
    add_library(iotdb_session STATIC
            ${SESSION_HANDWRITTEN_SRCS}
            ${THRIFT_GENERATED_SRCS})
else()
    add_library(iotdb_session SHARED
            ${SESSION_HANDWRITTEN_SRCS}
            ${THRIFT_GENERATED_SRCS})
endif()

add_dependencies(iotdb_session iotdb_thrift_external iotdb_thrift_codegen)

target_include_directories(iotdb_session
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/main>
            $<BUILD_INTERFACE:${THRIFT_GEN_CPP_DIR}>
            $<BUILD_INTERFACE:${THRIFT_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:include>)

if(BOOST_INCLUDE_DIR)
    target_include_directories(iotdb_session PUBLIC
            $<BUILD_INTERFACE:${BOOST_INCLUDE_DIR}>)
endif()

target_link_libraries(iotdb_session PUBLIC iotdb_thrift_static)

if(WITH_SSL)
    target_link_libraries(iotdb_session PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(iotdb_session PUBLIC WITH_SSL=1)
else()
    target_compile_definitions(iotdb_session PUBLIC WITH_SSL=0)
endif()

if(UNIX)
    target_link_libraries(iotdb_session PUBLIC pthread)
endif()

# ---------------------------------------------------------------------------
# Install rules - lay out the SDK exactly like the historical zip layout.
#     <prefix>/include/*.h          (handwritten + generated cpp headers)
#     <prefix>/include/thrift/...   (thrift runtime headers)
#     <prefix>/lib/<libiotdb_session.*>
#     <prefix>/lib/<libthrift.*>    (bundled so downstream users only need one tree)
# ---------------------------------------------------------------------------
include(GNUInstallDirs)

install(TARGETS iotdb_session
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/src/main/"
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")

install(DIRECTORY "${THRIFT_GEN_CPP_DIR}/"
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")

install(DIRECTORY "${THRIFT_INCLUDE_DIR}/thrift"
        DESTINATION include)

if(THRIFT_STATIC_LIB_PATH AND EXISTS "${THRIFT_STATIC_LIB_PATH}")
    install(FILES "${THRIFT_STATIC_LIB_PATH}" DESTINATION lib)
endif()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(src/test)
endif()

message(STATUS "iotdb_session configuration summary:")
message(STATUS "    WITH_SSL              = ${WITH_SSL}")
message(STATUS "    BUILD_TESTING         = ${BUILD_TESTING}")
message(STATUS "    IOTDB_OFFLINE         = ${IOTDB_OFFLINE}")
message(STATUS "    IOTDB_DEPS_DIR        = ${IOTDB_DEPS_DIR}")
message(STATUS "    BOOST_INCLUDE_DIR     = ${BOOST_INCLUDE_DIR}")
message(STATUS "    THRIFT_INCLUDE_DIR    = ${THRIFT_INCLUDE_DIR}")
message(STATUS "    THRIFT_STATIC_LIB     = ${THRIFT_STATIC_LIB_PATH}")
message(STATUS "    THRIFT_EXECUTABLE     = ${THRIFT_EXECUTABLE}")
message(STATUS "    CMAKE_INSTALL_PREFIX  = ${CMAKE_INSTALL_PREFIX}")
