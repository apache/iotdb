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

CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
CMAKE_POLICY(SET CMP0091 NEW)

PROJECT(iotdb_cpp_client_examples)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

IF(MSVC)
    # Match the IoTDB C++ SDK (/MD); same as a default Visual Studio application.
    SET(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
ENDIF()

set(IOTDB_SDK_ROOT "${CMAKE_SOURCE_DIR}/client"
    CACHE PATH "Unpacked IoTDB C++ SDK directory (contains include/ and lib/)")

if(NOT EXISTS "${IOTDB_SDK_ROOT}/include/Session.h"
        OR NOT EXISTS "${IOTDB_SDK_ROOT}/include/SessionC.h")
    file(GLOB _iotdb_sdk_dirs LIST_DIRECTORIES true "${IOTDB_SDK_ROOT}/iotdb-session-cpp-*")
    foreach(_iotdb_sdk_dir IN LISTS _iotdb_sdk_dirs)
        if(EXISTS "${_iotdb_sdk_dir}/include/Session.h"
                AND EXISTS "${_iotdb_sdk_dir}/include/SessionC.h")
            set(IOTDB_SDK_ROOT "${_iotdb_sdk_dir}"
                CACHE PATH "Unpacked IoTDB C++ SDK directory (contains include/ and lib/)" FORCE)
            break()
        endif()
    endforeach()
endif()

INCLUDE_DIRECTORIES("${IOTDB_SDK_ROOT}/include")

option(WITH_SSL "Build with SSL support" OFF)

IF(WITH_SSL)
    FIND_PACKAGE(OpenSSL REQUIRED)
    IF(OpenSSL_FOUND)
        MESSAGE(STATUS "OpenSSL found: ${OPENSSL_VERSION}")
        INCLUDE_DIRECTORIES(${OPENSSL_INCLUDE_DIR})
        ADD_DEFINITIONS(-DWITH_SSL=1)
    ELSE()
        MESSAGE(FATAL_ERROR "OpenSSL not found, but WITH_SSL is enabled")
    ENDIF()
ELSE()
    MESSAGE(STATUS "Building without SSL support")
    ADD_DEFINITIONS(-DWITH_SSL=0)
ENDIF()

if(WIN32)
    set(_iotdb_link_lib "${IOTDB_SDK_ROOT}/lib/iotdb_session.lib")
    set(_iotdb_runtime   "${IOTDB_SDK_ROOT}/lib/iotdb_session.dll")
    if(NOT EXISTS "${_iotdb_link_lib}" AND CMAKE_BUILD_TYPE)
        set(_iotdb_link_lib "${IOTDB_SDK_ROOT}/lib/${CMAKE_BUILD_TYPE}/iotdb_session.lib")
        set(_iotdb_runtime   "${IOTDB_SDK_ROOT}/lib/${CMAKE_BUILD_TYPE}/iotdb_session.dll")
    endif()
    if(NOT EXISTS "${_iotdb_link_lib}")
        set(_iotdb_link_lib "${IOTDB_SDK_ROOT}/lib/Release/iotdb_session.lib")
        set(_iotdb_runtime   "${IOTDB_SDK_ROOT}/lib/Release/iotdb_session.dll")
    endif()
    if(NOT EXISTS "${_iotdb_link_lib}")
        set(_iotdb_link_lib "${IOTDB_SDK_ROOT}/lib/Debug/iotdb_session.lib")
        set(_iotdb_runtime   "${IOTDB_SDK_ROOT}/lib/Debug/iotdb_session.dll")
    endif()
elseif(APPLE)
    set(_iotdb_link_lib "${IOTDB_SDK_ROOT}/lib/libiotdb_session.dylib")
    set(_iotdb_runtime   "${_iotdb_link_lib}")
else()
    set(_iotdb_link_lib "${IOTDB_SDK_ROOT}/lib/libiotdb_session.so")
    set(_iotdb_runtime   "${_iotdb_link_lib}")
endif()

if(NOT EXISTS "${_iotdb_link_lib}")
    message(FATAL_ERROR
        "IoTDB SDK not found at ${IOTDB_SDK_ROOT}. "
        "Unpack client-cpp-<version>-<classifier>.zip so that ${_iotdb_link_lib} exists.")
endif()

ADD_EXECUTABLE(SessionExample SessionExample.cpp)
ADD_EXECUTABLE(AlignedTimeseriesSessionExample AlignedTimeseriesSessionExample.cpp)
ADD_EXECUTABLE(MultiSvrNodeClient MultiSvrNodeClient.cpp)
ADD_EXECUTABLE(tree_example tree_example.c)

set(_example_targets
    SessionExample
    AlignedTimeseriesSessionExample
    MultiSvrNodeClient
    tree_example)

foreach(_t IN LISTS _example_targets)
  IF(WITH_SSL)
      TARGET_LINK_LIBRARIES(${_t} PRIVATE "${_iotdb_link_lib}" OpenSSL::SSL OpenSSL::Crypto)
  ELSE()
      TARGET_LINK_LIBRARIES(${_t} PRIVATE "${_iotdb_link_lib}")
  ENDIF()
  IF(UNIX)
      TARGET_LINK_LIBRARIES(${_t} PRIVATE pthread)
  ENDIF()

  # Run from the build output directory without setting LD_LIBRARY_PATH / PATH.
  if(UNIX)
      set_target_properties(${_t} PROPERTIES
          BUILD_RPATH "\$ORIGIN"
          INSTALL_RPATH "\$ORIGIN")
  endif()

  if(EXISTS "${_iotdb_runtime}")
      add_custom_command(TARGET ${_t} POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_if_different
                  "${_iotdb_runtime}" $<TARGET_FILE_DIR:${_t}>
          COMMENT "Copy IoTDB runtime library next to ${_t}")
  elseif(WIN32)
      message(WARNING "Missing ${_iotdb_runtime}; copy iotdb_session.dll manually before running ${_t}.")
  endif()
endforeach()

# Optional: stage a self-contained folder for copying to another machine (see README).
set(_example_dist_dir "${CMAKE_BINARY_DIR}/dist")
add_custom_target(example-dist DEPENDS ${_example_targets}
    COMMENT "Collect example binaries and IoTDB runtime into ${_example_dist_dir}")
foreach(_t IN LISTS _example_targets)
    add_custom_command(TARGET example-dist POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory "${_example_dist_dir}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:${_t}> "${_example_dist_dir}/"
        COMMENT "Stage ${_t}")
endforeach()
if(EXISTS "${_iotdb_runtime}")
    add_custom_command(TARGET example-dist POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_iotdb_runtime}" "${_example_dist_dir}/")
endif()
