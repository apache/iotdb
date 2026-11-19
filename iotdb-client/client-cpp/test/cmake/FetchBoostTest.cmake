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
# Run with cmake -DTEST_BINARY_DIR=<scratch-dir> -P <this-file> on Unix.
cmake_minimum_required(VERSION 3.16)

if(NOT TEST_BINARY_DIR)
    message(FATAL_ERROR "Set TEST_BINARY_DIR to a scratch directory")
endif()
get_filename_component(TEST_BINARY_DIR "${TEST_BINARY_DIR}" ABSOLUTE)
get_filename_component(_fetch_boost "${CMAKE_CURRENT_LIST_DIR}/../../cmake/FetchBoost.cmake" ABSOLUTE)
set(_keg "${TEST_BINARY_DIR}/Cellar/boost/1.90.0/include")
set(_shared "${TEST_BINARY_DIR}/include")
file(MAKE_DIRECTORY "${_keg}/boost" "${_shared}/openssl" "${TEST_BINARY_DIR}/modules")
file(WRITE "${_keg}/boost/version.hpp" "#define BOOST_VERSION 109000\n")
file(WRITE "${_shared}/openssl/ssl.h" "#error Unrelated OpenSSL headers must not be exposed by Boost\n")
file(CREATE_LINK "${_keg}/boost" "${_shared}/boost" SYMBOLIC)
file(WRITE "${TEST_BINARY_DIR}/modules/FindBoost.cmake" "
set(Boost_FOUND \${TEST_BOOST_FOUND})
set(Boost_INCLUDE_DIRS \${TEST_BOOST_INCLUDE_DIRS})
")
set(CMAKE_MODULE_PATH "${TEST_BINARY_DIR}/modules")
set(IOTDB_OFFLINE ON)

function(check_include source include_dir expected)
    unset(BOOST_INCLUDE_DIR CACHE)
    set(TEST_BOOST_FOUND FALSE)
    if(source STREQUAL "find_package")
        set(TEST_BOOST_FOUND TRUE)
        set(TEST_BOOST_INCLUDE_DIRS "${include_dir}")
    elseif(source STREQUAL "cache")
        set(BOOST_INCLUDE_DIR "${include_dir}" CACHE PATH "" FORCE)
    else()
        set(${source} "${include_dir}")
    endif()
    include("${_fetch_boost}")
    get_filename_component(expected "${expected}" REALPATH)
    if(NOT BOOST_INCLUDE_DIR STREQUAL expected)
        message(FATAL_ERROR "${source}: expected ${expected}, got ${BOOST_INCLUDE_DIR}")
    endif()
    # Reconfiguration must also retain the isolated path.
    include("${_fetch_boost}")
    if(NOT BOOST_INCLUDE_DIR STREQUAL expected)
        message(FATAL_ERROR "${source}: cached path changed to ${BOOST_INCLUDE_DIR}")
    endif()
    message(STATUS "PASS ${source}: ${include_dir}")
    unset(BOOST_INCLUDE_DIR CACHE)
endfunction()

foreach(source find_package cache BOOST_INCLUDE_DIR Boost_INCLUDE_DIR BOOST_INCLUDEDIR)
    check_include("${source}" "${_shared}" "${_keg}")
    check_include("${source}" "${_keg}" "${_keg}")
endforeach()
check_include(BOOST_ROOT "${TEST_BINARY_DIR}" "${_keg}")
