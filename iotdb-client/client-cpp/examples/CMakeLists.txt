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

# Windows SDK zips are x86_64; Visual Studio generators (especially VS2017) default to Win32.
IF(CMAKE_GENERATOR MATCHES "Visual Studio" AND NOT CMAKE_GENERATOR_PLATFORM)
    SET(CMAKE_GENERATOR_PLATFORM "x64" CACHE STRING "IoTDB C++ SDK is x64-only" FORCE)
ENDIF()

PROJECT(iotdb_cpp_client_examples)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

IF(MSVC)
    # Match the IoTDB C++ SDK (/MD); same as a default Visual Studio application.
    SET(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
ENDIF()

option(IOTDB_EXAMPLES_REGISTER_TESTS "Register runnable examples with ctest" OFF)

if(TARGET iotdb_session)
    set(_iotdb_examples_in_tree ON)
    set(_iotdb_link_lib iotdb_session)
else()
    set(_iotdb_examples_in_tree OFF)
    set(_iotdb_default_sdk_root "${CMAKE_CURRENT_LIST_DIR}/..")
    if(NOT EXISTS "${_iotdb_default_sdk_root}/include" OR NOT EXISTS "${_iotdb_default_sdk_root}/lib")
        set(_iotdb_default_sdk_root "${CMAKE_SOURCE_DIR}/client")
    endif()
    set(IOTDB_SDK_ROOT "${_iotdb_default_sdk_root}"
        CACHE PATH "Unpacked IoTDB C++ SDK directory (contains include/ and lib/)")

    if(NOT EXISTS "${IOTDB_SDK_ROOT}/include" OR NOT EXISTS "${IOTDB_SDK_ROOT}/lib")
        file(GLOB _iotdb_sdk_children LIST_DIRECTORIES true "${IOTDB_SDK_ROOT}/iotdb-session-cpp-*")
        list(LENGTH _iotdb_sdk_children _iotdb_sdk_child_count)
        if(_iotdb_sdk_child_count EQUAL 1)
            list(GET _iotdb_sdk_children 0 IOTDB_SDK_ROOT)
            message(STATUS "Using IoTDB SDK package root: ${IOTDB_SDK_ROOT}")
        endif()
    endif()

    INCLUDE_DIRECTORIES("${IOTDB_SDK_ROOT}/include")
endif()

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

if(NOT _iotdb_examples_in_tree)
    find_package(iotdb-session CONFIG QUIET
        PATHS "${IOTDB_SDK_ROOT}/cmake" "${IOTDB_SDK_ROOT}"
        NO_DEFAULT_PATH)
    if(WIN32)
        set(_iotdb_link_lib "${IOTDB_SDK_ROOT}/lib/iotdb_session.lib")
        set(_iotdb_runtime   "${IOTDB_SDK_ROOT}/lib/iotdb_session.dll")
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
            "Unpack iotdb-session-cpp-<version>-<classifier>.zip so that ${_iotdb_link_lib} exists.")
    endif()

    if(TARGET IoTDB::iotdb_session)
        set(_iotdb_link_lib IoTDB::iotdb_session)
    endif()
endif()

ADD_EXECUTABLE(SessionExample SessionExample.cpp)
ADD_EXECUTABLE(AlignedTimeseriesSessionExample AlignedTimeseriesSessionExample.cpp)
ADD_EXECUTABLE(TableModelSessionExample TableModelSessionExample.cpp)
ADD_EXECUTABLE(MultiSvrNodeClient MultiSvrNodeClient.cpp)
ADD_EXECUTABLE(tree_example tree_example.c)
ADD_EXECUTABLE(table_example table_example.c)

set(_example_targets
    SessionExample
    AlignedTimeseriesSessionExample
    TableModelSessionExample
    MultiSvrNodeClient
    tree_example
    table_example)

# OpenSSL runtime libraries bundled in the SDK lib/ (libssl / libcrypto). When
# building against an unpacked package, copy them next to each example binary so
# the examples run without a system OpenSSL - libiotdb_session records them as
# NEEDED and resolves them via its $ORIGIN runtime path.
set(_iotdb_sdk_ssl_runtime "")
if(NOT _iotdb_examples_in_tree)
    file(GLOB _iotdb_sdk_ssl_runtime
        "${IOTDB_SDK_ROOT}/lib/libssl*.so*"
        "${IOTDB_SDK_ROOT}/lib/libcrypto*.so*"
        "${IOTDB_SDK_ROOT}/lib/libssl*.dylib"
        "${IOTDB_SDK_ROOT}/lib/libcrypto*.dylib"
        "${IOTDB_SDK_ROOT}/lib/libssl*.dll"
        "${IOTDB_SDK_ROOT}/lib/libcrypto*.dll")
endif()

foreach(_t IN LISTS _example_targets)
  IF(WITH_SSL)
      TARGET_LINK_LIBRARIES(${_t} PRIVATE "${_iotdb_link_lib}" OpenSSL::SSL OpenSSL::Crypto)
  ELSE()
      TARGET_LINK_LIBRARIES(${_t} PRIVATE "${_iotdb_link_lib}")
  ENDIF()
  IF(UNIX)
      TARGET_LINK_LIBRARIES(${_t} PRIVATE pthread)
  ENDIF()

  # The packaged libiotdb_session records the bundled OpenSSL libs as DT_NEEDED;
  # point the linker at the SDK lib/ so it can resolve them without a system
  # OpenSSL present.
  if(UNIX AND NOT _iotdb_examples_in_tree)
      target_link_directories(${_t} PRIVATE "${IOTDB_SDK_ROOT}/lib")
  endif()

  # Run from the build output directory without setting LD_LIBRARY_PATH / PATH.
  if(UNIX)
      set_target_properties(${_t} PROPERTIES
          BUILD_RPATH "\$ORIGIN"
          INSTALL_RPATH "\$ORIGIN")
  endif()

  if(_iotdb_examples_in_tree)
      add_custom_command(TARGET ${_t} POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_if_different
                  $<TARGET_FILE:iotdb_session> $<TARGET_FILE_DIR:${_t}>
          COMMENT "Copy IoTDB runtime library next to ${_t}")
  elseif(EXISTS "${_iotdb_runtime}")
      add_custom_command(TARGET ${_t} POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_if_different
                  "${_iotdb_runtime}" $<TARGET_FILE_DIR:${_t}>
          COMMENT "Copy IoTDB runtime library next to ${_t}")
      foreach(_ssl_lib IN LISTS _iotdb_sdk_ssl_runtime)
          add_custom_command(TARGET ${_t} POST_BUILD
              COMMAND ${CMAKE_COMMAND} -E copy_if_different
                      "${_ssl_lib}" $<TARGET_FILE_DIR:${_t}>
              COMMENT "Copy bundled OpenSSL runtime next to ${_t}")
      endforeach()
  elseif(WIN32)
      message(WARNING "Missing ${_iotdb_runtime}; copy iotdb_session.dll manually before running ${_t}.")
  endif()
endforeach()

# Optional: stage a self-contained folder for copying to another machine (see package README).
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
# Stage the bundled OpenSSL runtime too, so a copied dist/ runs on a machine
# without a system OpenSSL.
foreach(_ssl_lib IN LISTS _iotdb_sdk_ssl_runtime)
    add_custom_command(TARGET example-dist POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_ssl_lib}" "${_example_dist_dir}/")
endforeach()

if(IOTDB_EXAMPLES_REGISTER_TESTS)
    set(_runnable_example_targets
        SessionExample
        AlignedTimeseriesSessionExample
        TableModelSessionExample
        tree_example
        table_example)
    foreach(_t IN LISTS _runnable_example_targets)
        add_test(NAME example_${_t} COMMAND ${_t})
    endforeach()
    set_tests_properties(
        example_SessionExample
        example_AlignedTimeseriesSessionExample
        example_TableModelSessionExample
        example_tree_example
        example_table_example
        PROPERTIES RUN_SERIAL TRUE)
endif()
