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

cmake_minimum_required(VERSION 3.15)
project(iotdb_session CXX C)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_POLICY_DEFAULT_CMP0091 NEW)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(IOTDB_EXTRA_CXX_FLAGS ""
    CACHE STRING "Extra flags appended to CMAKE_CXX_FLAGS")
set(IOTDB_USE_CXX11_ABI ""
    CACHE STRING "Set _GLIBCXX_USE_CXX11_ABI for GNU libstdc++ builds (empty keeps toolchain default)")

if(NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
    if(IOTDB_EXTRA_CXX_FLAGS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${IOTDB_EXTRA_CXX_FLAGS}")
    endif()
endif()

if(MSVC)
    # /MD: matches default Visual Studio projects; CRT lives in the VC redistributable.
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
endif()

if(NOT IOTDB_USE_CXX11_ABI STREQUAL "" AND NOT IOTDB_USE_CXX11_ABI MATCHES "^[01]$")
    message(FATAL_ERROR "IOTDB_USE_CXX11_ABI must be empty, 0, or 1")
endif()
if(NOT MSVC)
    if(NOT IOTDB_USE_CXX11_ABI STREQUAL "")
        set(_iotdb_cxx11_abi_stamp_value "${IOTDB_USE_CXX11_ABI}")
        add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=${IOTDB_USE_CXX11_ABI})
    else()
        set(_iotdb_cxx11_abi_stamp_value "default")
    endif()
    set(_iotdb_cxx11_abi_stamp "${CMAKE_BINARY_DIR}/.iotdb-cxx11-abi")
    if(EXISTS "${_iotdb_cxx11_abi_stamp}")
        file(READ "${_iotdb_cxx11_abi_stamp}" _iotdb_previous_cxx11_abi)
    else()
        set(_iotdb_previous_cxx11_abi "")
    endif()
    if(NOT _iotdb_previous_cxx11_abi STREQUAL _iotdb_cxx11_abi_stamp_value)
        message(STATUS
                "IOTDB_USE_CXX11_ABI changed from '${_iotdb_previous_cxx11_abi}' "
                "to '${_iotdb_cxx11_abi_stamp_value}'; removing cached iotdb_session objects")
        file(REMOVE_RECURSE "${CMAKE_BINARY_DIR}/CMakeFiles/iotdb_session.dir")
        file(GLOB _iotdb_stale_session_libs "${CMAKE_BINARY_DIR}/libiotdb_session.so*")
        if(_iotdb_stale_session_libs)
            file(REMOVE ${_iotdb_stale_session_libs})
        endif()
    endif()
    file(WRITE "${_iotdb_cxx11_abi_stamp}" "${_iotdb_cxx11_abi_stamp_value}")
endif()

option(WITH_SSL "Build with OpenSSL support" ON)
option(BUILD_TESTING "Build IT test executables" OFF)
option(IOTDB_OFFLINE "Disable all network access during configure" OFF)
set(IOTDB_SESSION_VERSION "0.0.0"
    CACHE STRING "IoTDB C++ session package version")
string(REGEX MATCH "^[0-9]+" IOTDB_SESSION_SOVERSION "${IOTDB_SESSION_VERSION}")
if(NOT IOTDB_SESSION_SOVERSION)
    set(IOTDB_SESSION_SOVERSION "0")
endif()

set(IOTDB_DEPS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    CACHE PATH "Local tarball cache for third-party dependencies (lives under client-cpp/)")
if(APPLE)
    set(_iotdb_default_boost_version "1.84.0")
else()
    set(_iotdb_default_boost_version "1.60.0")
endif()
set(BOOST_VERSION "${_iotdb_default_boost_version}"
    CACHE STRING "Boost version used when downloading / unpacking (Thrift build only)")
set(THRIFT_VERSION "0.23.0"
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
    include(InstallOpenSSLRuntime)
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

if(UNIX AND NOT APPLE)
    set_target_properties(iotdb_session PROPERTIES
            VERSION "${IOTDB_SESSION_VERSION}"
            SOVERSION "${IOTDB_SESSION_SOVERSION}")
endif()

# When SSL is on we bundle the OpenSSL shared libraries next to libiotdb_session
# in the package lib/ directory. Give the library an $ORIGIN-relative runtime
# search path so the loader finds them without LD_LIBRARY_PATH / install_name
# tweaks, keeping the SDK self-contained.
if(WITH_SSL)
    if(APPLE)
        set_target_properties(iotdb_session PROPERTIES
                BUILD_RPATH "@loader_path"
                INSTALL_RPATH "@loader_path")
    elseif(UNIX)
        set_target_properties(iotdb_session PROPERTIES
                BUILD_RPATH "$ORIGIN"
                INSTALL_RPATH "$ORIGIN")
    endif()
endif()

add_dependencies(iotdb_session iotdb_thrift_external iotdb_thrift_codegen)

target_compile_definitions(iotdb_session PRIVATE THRIFT_STATIC_DEFINE IOTDB_BUILDING_SHARED)
if(NOT IOTDB_USE_CXX11_ABI STREQUAL "" AND NOT MSVC)
    target_compile_definitions(iotdb_session PUBLIC
            _GLIBCXX_USE_CXX11_ABI=${IOTDB_USE_CXX11_ABI})
endif()

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
    # whole-archive pulls all Thrift objects into libiotdb_session.so; allow-multiple-definition
    # avoids libgcc __morestack_* duplicate symbol errors on some GCC/toolchain combos.
    target_link_libraries(iotdb_session PRIVATE
            -Wl,--whole-archive
            ${THRIFT_STATIC_LIB_PATH}
            -Wl,--no-whole-archive
            -Wl,--allow-multiple-definition)
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

if(IOTDB_USE_CXX11_ABI STREQUAL "1" AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_custom_command(TARGET iotdb_session POST_BUILD
            COMMAND ${CMAKE_COMMAND}
                    -DIOTDB_SESSION_LIBRARY=$<TARGET_FILE:iotdb_session>
                    -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckCxx11Abi.cmake"
            COMMENT "Verifying _GLIBCXX_USE_CXX11_ABI=1 symbols in libiotdb_session")
endif()

include(GNUInstallDirs)

set(IOTDB_PUBLIC_HEADERS
        Export.h
        SessionConfig.h
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

# Ship the OpenSSL shared libraries we link against next to iotdb_session so the
# packaged SDK is self-contained on machines without a system OpenSSL.
if(WITH_SSL)
    iotdb_install_openssl_runtime()
endif()

foreach(_hdr IN LISTS IOTDB_PUBLIC_HEADERS)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/src/include/${_hdr}"
            DESTINATION include)
endforeach()

set(IOTDB_SESSION_PC_LIBS "-liotdb_session")
set(IOTDB_SESSION_PC_CFLAGS "")
set(IOTDB_SESSION_CXX11_ABI_COMPILE_DEFINITION "")
if(UNIX)
    set(IOTDB_SESSION_PC_LIBS "${IOTDB_SESSION_PC_LIBS} -pthread")
endif()
if(NOT IOTDB_USE_CXX11_ABI STREQUAL "" AND NOT MSVC)
    set(IOTDB_SESSION_PC_CFLAGS "-D_GLIBCXX_USE_CXX11_ABI=${IOTDB_USE_CXX11_ABI}")
    set(IOTDB_SESSION_CXX11_ABI_COMPILE_DEFINITION
            "_GLIBCXX_USE_CXX11_ABI=${IOTDB_USE_CXX11_ABI}")
endif()
configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/iotdb-session-config.cmake.in"
        "${CMAKE_BINARY_DIR}/package-metadata/cmake/iotdb-session-config.cmake"
        @ONLY)
configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/iotdb-session.pc.in"
        "${CMAKE_BINARY_DIR}/package-metadata/pkgconfig/iotdb-session.pc"
        @ONLY)
execute_process(
        COMMAND git rev-parse HEAD
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        OUTPUT_VARIABLE IOTDB_SESSION_GIT_COMMIT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
if(NOT IOTDB_SESSION_GIT_COMMIT)
    set(IOTDB_SESSION_GIT_COMMIT "unknown")
endif()
string(TIMESTAMP IOTDB_SESSION_BUILD_TIME "%Y-%m-%dT%H:%M:%SZ" UTC)
set(IOTDB_SESSION_CI_BUILD_ID "$ENV{GITHUB_RUN_ID}")
if(NOT IOTDB_SESSION_CI_BUILD_ID)
    set(IOTDB_SESSION_CI_BUILD_ID "local")
endif()
file(WRITE "${CMAKE_BINARY_DIR}/package-metadata/VERSION"
        "${IOTDB_SESSION_VERSION}\n")
file(WRITE "${CMAKE_BINARY_DIR}/package-metadata/BUILD-INFO.txt"
        "Apache IoTDB C++ session client build information\n\n"
        "version=${IOTDB_SESSION_VERSION}\n"
        "commit=${IOTDB_SESSION_GIT_COMMIT}\n"
        "build.time=${IOTDB_SESSION_BUILD_TIME}\n"
        "ci.build.id=${IOTDB_SESSION_CI_BUILD_ID}\n"
        "compiler=${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}\n"
        "cmake.generator=${CMAKE_GENERATOR}\n"
        "cmake.build.type=${CMAKE_BUILD_TYPE}\n"
        "with.ssl=${WITH_SSL}\n"
        "iotdb.offline=${IOTDB_OFFLINE}\n"
        "iotdb.use.cxx11.abi=${IOTDB_USE_CXX11_ABI}\n"
        "iotdb.extra.cxx.flags=${IOTDB_EXTRA_CXX_FLAGS}\n")
install(FILES "${CMAKE_BINARY_DIR}/package-metadata/cmake/iotdb-session-config.cmake"
        DESTINATION cmake)
install(FILES "${CMAKE_BINARY_DIR}/package-metadata/pkgconfig/iotdb-session.pc"
        DESTINATION pkgconfig)
install(FILES
        "${CMAKE_BINARY_DIR}/package-metadata/VERSION"
        "${CMAKE_BINARY_DIR}/package-metadata/BUILD-INFO.txt"
        DESTINATION .)

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(test)
    set(IOTDB_EXAMPLES_REGISTER_TESTS ON)
    add_subdirectory(examples)
endif()

message(STATUS "iotdb_session configuration summary:")
message(STATUS "    WITH_SSL              = ${WITH_SSL}")
message(STATUS "    BUILD_TESTING         = ${BUILD_TESTING}")
message(STATUS "    IOTDB_OFFLINE         = ${IOTDB_OFFLINE}")
message(STATUS "    IOTDB_USE_CXX11_ABI   = ${IOTDB_USE_CXX11_ABI}")
message(STATUS "    IOTDB_DEPS_DIR        = ${IOTDB_DEPS_DIR}")
message(STATUS "    BOOST_INCLUDE_DIR     = ${BOOST_INCLUDE_DIR} (Thrift build only)")
message(STATUS "    THRIFT_INCLUDE_DIR    = ${THRIFT_INCLUDE_DIR}")
message(STATUS "    THRIFT_STATIC_LIB     = ${THRIFT_STATIC_LIB_PATH}")
message(STATUS "    THRIFT_EXECUTABLE     = ${THRIFT_EXECUTABLE}")
message(STATUS "    CMAKE_INSTALL_PREFIX  = ${CMAKE_INSTALL_PREFIX}")
