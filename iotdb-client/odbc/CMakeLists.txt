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
cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0091 NEW)
project(apache_iotdb_odbc VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_SIZEOF_VOID_P EQUAL 8 OR
   NOT CMAKE_SYSTEM_NAME MATCHES "^(Windows|Linux)$" OR
   NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(AMD64|amd64|x86_64|X86_64)$" OR
   CMAKE_GENERATOR_PLATFORM MATCHES "[Aa][Rr][Mm]")
    message(FATAL_ERROR "ODBC release builds support Windows/Linux x64 only")
endif()
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
if(MSVC)
    # OpenSSL and Thrift are configured once per build directory. Avoid linking
    # a Debug driver against dependencies built with the Release CRT.
    set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE}" CACHE STRING "Configurations" FORCE)
endif()
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
option(ODBC_BUILD_TESTS "Build ODBC tests" ON)
option(ODBC_ENABLE_LOGGING "Compile file logging support into the ODBC driver" OFF)
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/../pom.xml" _version_line REGEX "<version>")
list(GET _version_line 0 _version_line)
string(REGEX REPLACE ".*<version>([^<]+)</version>.*" "\\1" _repository_version "${_version_line}")
set(ODBC_PACKAGE_VERSION "${_repository_version}" CACHE STRING "Package version")
set(ODBC_PACKAGE_CLASSIFIER "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}" CACHE STRING "Release platform")

# Scope SDK configuration to this build; its standalone defaults remain unchanged.
set(IOTDB_SESSION_STATIC ON CACHE BOOL "" FORCE)
set(IOTDB_SESSION_INSTALL OFF CACHE BOOL "" FORCE)
set(IOTDB_STATIC_CRT ON CACHE BOOL "" FORCE)
set(IOTDB_OPENSSL_STATIC ON CACHE BOOL "" FORCE)
set(IOTDB_OPENSSL_FROM_SOURCE ON CACHE BOOL "" FORCE)
set(WITH_SSL ON CACHE BOOL "" FORCE)
set(IOTDB_SESSION_VERSION "${ODBC_PACKAGE_VERSION}" CACHE STRING "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(../client-cpp client-cpp EXCLUDE_FROM_ALL)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(FetchDependencies)

set(DRIVER_VERSION "${ODBC_PACKAGE_VERSION}")
set(DEBUG_LOG_OUTPUT_PATH "apache_iotdb_odbc.log")
configure_file(src/DriverConfig.h.in generated/DriverConfig.h @ONLY)
set(ODBC_SOURCES
    src/Catalog.cpp
    src/ConnectionHandle.cpp
    src/CopyField.cpp
    src/DiagnosticManager.cpp
    src/EnvironmentHandle.cpp
    src/Log.cpp
    src/ODBCField.cpp
    src/ODBCHandle.cpp
    src/ODBCRestResultSet.cpp
    src/ODBCResultSet.cpp
    src/ODBCSessionResultSet.cpp
    src/StatementHandle.cpp
    src/StatementUtils.cpp
    src/Unicode.cpp
    src/driver.cpp
    src/rest_api_client.cpp
    src/session_api_client.cpp)
add_library(odbc_objects OBJECT ${ODBC_SOURCES})
target_include_directories(odbc_objects PUBLIC src "${CMAKE_CURRENT_BINARY_DIR}/generated")
target_link_libraries(odbc_objects PUBLIC iotdb_session CURL::libcurl nlohmann_json::nlohmann_json)
target_compile_definitions(odbc_objects PUBLIC CURL_STATICLIB)
add_library(apache_iotdb_odbc SHARED $<TARGET_OBJECTS:odbc_objects>)
target_link_libraries(apache_iotdb_odbc PRIVATE odbc_objects)
if(WIN32)
    enable_language(RC)
    target_compile_definitions(odbc_objects PUBLIC NOMINMAX WIN32_LEAN_AND_MEAN)
    target_compile_options(odbc_objects PUBLIC /utf-8)
    target_sources(odbc_objects PRIVATE src/setup/setup_dialog.cpp)
    target_sources(apache_iotdb_odbc PRIVATE src/setup/setup.rc src/driver.def)
    target_link_libraries(odbc_objects PUBLIC odbccp32 legacy_stdio_definitions)
    add_executable(install_driver src/install_driver.cpp)
    target_compile_definitions(install_driver PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
    target_link_libraries(install_driver PRIVATE odbccp32 legacy_stdio_definitions)
    install(TARGETS install_driver RUNTIME DESTINATION bin)
    install(FILES scripts/windows/install.bat scripts/windows/uninstall.bat DESTINATION .)
else()
    find_path(ODBC_INCLUDE_DIR sql.h REQUIRED)
    find_library(ODBCINST_LIBRARY odbcinst REQUIRED)
    target_include_directories(odbc_objects PUBLIC "${ODBC_INCLUDE_DIR}")
    target_link_libraries(odbc_objects PUBLIC "${ODBCINST_LIBRARY}")
    target_link_options(apache_iotdb_odbc PRIVATE -static-libstdc++ -static-libgcc
        "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/cmake/exports.map"
        "LINKER:--exclude-libs,ALL" "LINKER:-z,defs" "LINKER:-Bsymbolic-functions")
    set_property(TARGET apache_iotdb_odbc APPEND PROPERTY LINK_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/exports.map")
endif()
install(TARGETS apache_iotdb_odbc RUNTIME DESTINATION bin LIBRARY DESTINATION lib)
install(FILES README.md DESTINATION .)
install(DIRECTORY conf/ DESTINATION conf)
install(FILES "${odbc_curl_SOURCE_DIR}/COPYING" DESTINATION licenses RENAME curl.txt)
install(FILES "${odbc_json_SOURCE_DIR}/LICENSE.MIT" DESTINATION licenses RENAME json.txt)
install(DIRECTORY ../client-cpp/src/assembly/package-metadata/third_party/ DESTINATION licenses/sdk)
install(FILES ../../LICENSE ../../NOTICE DESTINATION .)
install(DIRECTORY licenses/ DESTINATION licenses)
get_filename_component(ODBC_REPOSITORY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." REALPATH)
# Container builds read the host checkout as root. Trust only this configured
# source repository for the provenance query, without changing global Git policy.
execute_process(COMMAND git -c "safe.directory=${ODBC_REPOSITORY_ROOT}" rev-parse HEAD
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    OUTPUT_VARIABLE ODBC_COMMIT OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
if(NOT ODBC_COMMIT)
    set(ODBC_COMMIT "unknown")
endif()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/BUILD-INFO.txt"
    "version=${ODBC_PACKAGE_VERSION}\ncommit=${ODBC_COMMIT}\n"
    "platform=${ODBC_PACKAGE_CLASSIFIER}\ncompiler=${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}\n"
    "thrift=${THRIFT_VERSION}\nopenssl=${OPENSSL_FALLBACK_VERSION}\n"
    "curl=${ODBC_CURL_VERSION}\njson=${ODBC_JSON_VERSION}\n"
    "session.linkage=static\nopenssl.linkage=static\ncurl.linkage=static\n")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/BUILD-INFO.txt" DESTINATION .)

if(ODBC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

set(CPACK_GENERATOR ZIP)
set(CPACK_PACKAGE_FILE_NAME "iotdb-odbc-${ODBC_PACKAGE_VERSION}-${ODBC_PACKAGE_CLASSIFIER}")
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY ON)
set(CPACK_PACKAGE_CHECKSUM SHA512)
include(CPack)
