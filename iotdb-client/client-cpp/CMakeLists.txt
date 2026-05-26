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

cmake_minimum_required(VERSION 3.16)
project(iotdb_session CXX C)

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
    # /MD: matches default Visual Studio projects; CRT lives in the VC redistributable.
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
endif()

option(WITH_SSL "Build with OpenSSL support" OFF)
option(BUILD_TESTING "Build IT test executables" OFF)
option(IOTDB_OFFLINE "Disable all network access during configure" OFF)

set(IOTDB_DEPS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    CACHE PATH "Local tarball cache for third-party dependencies (lives under client-cpp/)")
set(BOOST_VERSION "1.84.0"
    CACHE STRING "Boost version used when downloading / unpacking (Thrift build only)")
set(THRIFT_VERSION "0.21.0"
    CACHE STRING "Apache Thrift version used when downloading / building")

if(WIN32)
    set(IOTDB_OS_DEPS_DIR "${IOTDB_DEPS_DIR}/windows")
elseif(APPLE)
    set(IOTDB_OS_DEPS_DIR "${IOTDB_DEPS_DIR}/mac")
else()
    set(IOTDB_OS_DEPS_DIR "${IOTDB_DEPS_DIR}/linux")
endif()
file(MAKE_DIRECTORY "${IOTDB_OS_DEPS_DIR}")

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/install"
        CACHE PATH "Install prefix" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(FetchBoost)        # -> BOOST_INCLUDE_DIR (Thrift build only)
include(FetchBuildTools)
if(WITH_SSL)
    include(FetchOpenSSL)
endif()
include(FetchThrift)
include(GenerateThriftSources)

file(GLOB SESSION_PUBLIC_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/session/*.cpp")
file(GLOB SESSION_RPC_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/rpc/*.cpp")

add_library(iotdb_session SHARED
        ${SESSION_PUBLIC_SRCS}
        ${SESSION_RPC_SRCS}
        ${THRIFT_GENERATED_SRCS})

if(WIN32)
    set_target_properties(iotdb_session PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()

add_dependencies(iotdb_session iotdb_thrift_external iotdb_thrift_codegen)

target_compile_definitions(iotdb_session PRIVATE THRIFT_STATIC_DEFINE IOTDB_BUILDING_SHARED)

target_include_directories(iotdb_session
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/include>
            $<INSTALL_INTERFACE:include>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src/rpc
            ${THRIFT_GEN_CPP_DIR}
            ${THRIFT_INCLUDE_DIR}
            $<$<BOOL:${BOOST_INCLUDE_DIR}>:${BOOST_INCLUDE_DIR}>)

if(APPLE)
    target_link_libraries(iotdb_session PRIVATE "-Wl,-force_load,${THRIFT_STATIC_LIB_PATH}")
elseif(UNIX AND NOT MSVC)
    # Link only required Thrift objects into the shared library. --whole-archive can
    # pull duplicate libgcc morestack symbols on modern GCC (split-stack).
    target_link_libraries(iotdb_session PRIVATE ${THRIFT_STATIC_LIB_PATH})
else()
    target_link_libraries(iotdb_session PRIVATE iotdb_thrift_static)
endif()

if(WITH_SSL)
    target_link_libraries(iotdb_session PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(iotdb_session PUBLIC WITH_SSL=1)
else()
    target_compile_definitions(iotdb_session PUBLIC WITH_SSL=0)
endif()

if(UNIX)
    target_link_libraries(iotdb_session PUBLIC pthread)
endif()

include(GNUInstallDirs)

set(IOTDB_PUBLIC_HEADERS
        Export.h
        Session.h
        Common.h
        Optional.h
        Date.h
        Status.h
        Endpoint.h
        SessionBuilder.h
        AbstractSessionBuilder.h
        TableSession.h
        TableSessionBuilder.h
        SessionC.h
        SessionDataSet.h
        DeviceID.h
        Column.h
        ColumnDecoder.h
        TsBlock.h)

# Windows: RUNTIME = iotdb_session.dll, ARCHIVE = import .lib (both under lib/ in the zip).
install(TARGETS iotdb_session
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

foreach(_hdr IN LISTS IOTDB_PUBLIC_HEADERS)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/src/include/${_hdr}"
            DESTINATION include)
endforeach()

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(src/test)
endif()

message(STATUS "iotdb_session configuration summary:")
message(STATUS "    WITH_SSL              = ${WITH_SSL}")
message(STATUS "    BUILD_TESTING         = ${BUILD_TESTING}")
message(STATUS "    IOTDB_OFFLINE         = ${IOTDB_OFFLINE}")
message(STATUS "    IOTDB_DEPS_DIR        = ${IOTDB_DEPS_DIR}")
message(STATUS "    BOOST_INCLUDE_DIR     = ${BOOST_INCLUDE_DIR} (Thrift build only)")
message(STATUS "    THRIFT_INCLUDE_DIR    = ${THRIFT_INCLUDE_DIR}")
message(STATUS "    THRIFT_STATIC_LIB     = ${THRIFT_STATIC_LIB_PATH}")
message(STATUS "    THRIFT_EXECUTABLE     = ${THRIFT_EXECUTABLE}")
message(STATUS "    CMAKE_INSTALL_PREFIX  = ${CMAKE_INSTALL_PREFIX}")
