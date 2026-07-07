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

# Match the SDK default (WITH_SSL=ON in the main client build). When this
# directory is added via add_subdirectory(), the parent cache value wins.
option(WITH_SSL "Build with SSL/TLS support" ON)

set(_iotdb_use_bundled_ssl OFF)
set(_iotdb_ssl_link_libs "")

if(NOT _iotdb_examples_in_tree)
    file(GLOB _iotdb_bundled_ssl_runtime
        "${IOTDB_SDK_ROOT}/lib/libssl*.so*"
        "${IOTDB_SDK_ROOT}/lib/libcrypto*.so*"
        "${IOTDB_SDK_ROOT}/lib/libssl*.dylib"
        "${IOTDB_SDK_ROOT}/lib/libcrypto*.dylib"
        "${IOTDB_SDK_ROOT}/lib/libssl*.dll"
        "${IOTDB_SDK_ROOT}/lib/libcrypto*.dll")
    if(_iotdb_bundled_ssl_runtime)
        set(_iotdb_use_bundled_ssl ON)
        set(WITH_SSL ON CACHE BOOL "Build with SSL/TLS support" FORCE)
        message(STATUS "Using bundled Tongsuo/OpenSSL-compatible libraries from ${IOTDB_SDK_ROOT}/lib")
    endif()
endif()

if(WITH_SSL)
    if(_iotdb_examples_in_tree)
        add_compile_definitions(WITH_SSL=1)
    elseif(_iotdb_use_bundled_ssl)
        add_compile_definitions(WITH_SSL=1)
        if(UNIX)
            find_library(_iotdb_ssl_lib NAMES ssl libssl
                PATHS "${IOTDB_SDK_ROOT}/lib" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
            find_library(_iotdb_crypto_lib NAMES crypto libcrypto
                PATHS "${IOTDB_SDK_ROOT}/lib" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
            if(_iotdb_ssl_lib AND _iotdb_crypto_lib)
                set(_iotdb_ssl_link_libs "${_iotdb_ssl_lib}" "${_iotdb_crypto_lib}")
            else()
                message(FATAL_ERROR
                    "Bundled libssl/libcrypto not found under ${IOTDB_SDK_ROOT}/lib")
            endif()
        endif()
    else()
        message(FATAL_ERROR
            "WITH_SSL=ON requires building inside the IoTDB client tree, or an SDK "
            "that bundles libssl/libcrypto under ${IOTDB_SDK_ROOT}/lib. "
            "Pass -DWITH_SSL=OFF only for SDKs built without SSL.")
    endif()
else()
    message(STATUS "Building without SSL support")
    add_compile_definitions(WITH_SSL=0)
endif()

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

set(_it_plain_examples "")
set(_it_ssl_examples "")
set(_it_ntls_examples "")

if(_iotdb_examples_in_tree)
    ADD_EXECUTABLE(cpp_tree_example cpp_tree_example.cpp)
    ADD_EXECUTABLE(cpp_table_example cpp_table_example.cpp)

    list(APPEND _example_targets
        cpp_tree_example
        cpp_table_example)

    set(_it_plain_examples
        cpp_tree_example
        cpp_table_example
        tree_example
        table_example)
endif()

if(WITH_SSL AND (_iotdb_examples_in_tree OR _iotdb_use_bundled_ssl))
    ADD_EXECUTABLE(cpp_tls_example cpp_tls_example.cpp ExampleTlsConfig.cpp)
    ADD_EXECUTABLE(cpp_ntls_example cpp_ntls_example.cpp ExampleNtlsHandshake.cpp)
    ADD_EXECUTABLE(tls_tree_example tls_tree_example.c ExampleTlsConfig.cpp)
    ADD_EXECUTABLE(c_ntls_example c_ntls_example.c ExampleNtlsHandshake.cpp)

    list(APPEND _example_targets
        cpp_tls_example
        cpp_ntls_example
        tls_tree_example
        c_ntls_example)

    if(_iotdb_examples_in_tree)
        set(_it_ssl_examples
            cpp_tls_example
            tls_tree_example)

        set(_it_ntls_examples
            cpp_ntls_example
            c_ntls_example)
    endif()
endif()

foreach(_t IN LISTS _example_targets)
  if(WITH_SSL AND _iotdb_ssl_link_libs)
      target_link_libraries(${_t} PRIVATE "${_iotdb_link_lib}" ${_iotdb_ssl_link_libs})
  else()
      target_link_libraries(${_t} PRIVATE "${_iotdb_link_lib}")
  endif()
  IF(UNIX)
      TARGET_LINK_LIBRARIES(${_t} PRIVATE pthread)
  ENDIF()

  # The packaged libiotdb_session records the bundled SSL libs as DT_NEEDED; point
  # the linker at the SDK lib/ so it can resolve them without a system install.
  if(UNIX AND NOT _iotdb_examples_in_tree AND _iotdb_use_bundled_ssl)
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
      if(WIN32 AND WITH_SSL)
          _iotdb_collect_openssl_windows_dlls(_iotdb_ssl_runtime_dlls)
          foreach(_ssl_dll IN LISTS _iotdb_ssl_runtime_dlls)
              add_custom_command(TARGET ${_t} POST_BUILD
                  COMMAND ${CMAKE_COMMAND} -E copy_if_different
                          "${_ssl_dll}" $<TARGET_FILE_DIR:${_t}>
                  COMMENT "Copy bundled SSL runtime next to ${_t}")
          endforeach()
      endif()
  elseif(EXISTS "${_iotdb_runtime}")
      add_custom_command(TARGET ${_t} POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_if_different
                  "${_iotdb_runtime}" $<TARGET_FILE_DIR:${_t}>
          COMMENT "Copy IoTDB runtime library next to ${_t}")
      foreach(_ssl_lib IN LISTS _iotdb_bundled_ssl_runtime)
          add_custom_command(TARGET ${_t} POST_BUILD
              COMMAND ${CMAKE_COMMAND} -E copy_if_different
                      "${_ssl_lib}" $<TARGET_FILE_DIR:${_t}>
              COMMENT "Copy bundled SSL runtime next to ${_t}")
      endforeach()
  elseif(WIN32)
      message(WARNING "Missing ${_iotdb_runtime}; copy iotdb_session.dll manually before running ${_t}.")
  endif()
endforeach()

if(_iotdb_examples_in_tree AND WITH_SSL AND IOTDB_EXAMPLES_REGISTER_TESTS)
    file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../test/fixtures" _iotdb_example_fixtures_dir)
    string(REPLACE "\\" "/" _iotdb_example_fixtures_dir_fwd "${_iotdb_example_fixtures_dir}")
    foreach(_t IN LISTS _it_ssl_examples _it_ntls_examples)
        target_compile_definitions(${_t} PRIVATE
                IOTDB_TEST_FIXTURES_DIR="${_iotdb_example_fixtures_dir_fwd}")
        add_custom_command(TARGET ${_t} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_directory
                        "${CMAKE_CURRENT_SOURCE_DIR}/../test/fixtures"
                        "$<TARGET_FILE_DIR:${_t}>/fixtures"
                COMMENT "Copy SSL test fixtures next to ${_t}")
    endforeach()
    foreach(_t IN LISTS _it_ntls_examples)
        target_sources(${_t} PRIVATE
                "${CMAKE_CURRENT_SOURCE_DIR}/../test/cpp/SslTestFixtures.cpp")
        target_include_directories(${_t} PRIVATE
                "${CMAKE_CURRENT_SOURCE_DIR}/../test/cpp"
                "${CMAKE_CURRENT_SOURCE_DIR}/../src/rpc"
                "${THRIFT_GEN_CPP_DIR}"
                "${THRIFT_INCLUDE_DIR}")
        if(BOOST_INCLUDE_DIR)
            target_include_directories(${_t} PRIVATE "${BOOST_INCLUDE_DIR}")
        endif()
        if(BOOST_INCLUDE_DIR)
            target_include_directories(${_t} PRIVATE "${BOOST_INCLUDE_DIR}")
        endif()
        file(TO_CMAKE_PATH "${OPENSSL_ROOT_DIR}" _iotdb_openssl_root_dir_cmake)
        string(REPLACE "\\" "/" _iotdb_openssl_root_dir_fwd "${_iotdb_openssl_root_dir_cmake}")
        if(WIN32)
            file(TO_CMAKE_PATH "${OPENSSL_ROOT_DIR}/bin/openssl.exe" _iotdb_openssl_executable_cmake)
            string(REPLACE "\\" "/" _iotdb_openssl_executable_fwd "${_iotdb_openssl_executable_cmake}")
            target_compile_definitions(${_t} PRIVATE
                    IOTDB_OPENSSL_EXECUTABLE="${_iotdb_openssl_executable_fwd}"
                    IOTDB_OPENSSL_ROOT_DIR="${_iotdb_openssl_root_dir_fwd}")
            target_link_libraries(${_t} PRIVATE ws2_32 "${THRIFT_STATIC_LIB_PATH}")
        else()
            file(TO_CMAKE_PATH "${OPENSSL_ROOT_DIR}/bin/openssl" _iotdb_openssl_executable_cmake)
            string(REPLACE "\\" "/" _iotdb_openssl_executable_fwd "${_iotdb_openssl_executable_cmake}")
            target_compile_definitions(${_t} PRIVATE
                    IOTDB_OPENSSL_EXECUTABLE="${_iotdb_openssl_executable_fwd}"
                    IOTDB_OPENSSL_ROOT_DIR="${_iotdb_openssl_root_dir_fwd}")
            target_link_libraries(${_t} PRIVATE iotdb_thrift_static)
        endif()
    endforeach()
endif()

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
# Stage the bundled SSL runtime too, so a copied dist/ runs without a system SSL.
foreach(_ssl_lib IN LISTS _iotdb_bundled_ssl_runtime)
    add_custom_command(TARGET example-dist POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_ssl_lib}" "${_example_dist_dir}/")
endforeach()

if(IOTDB_EXAMPLES_REGISTER_TESTS)
    foreach(_t IN LISTS _it_plain_examples)
        add_test(NAME example_${_t} COMMAND ${_t})
        set_tests_properties(example_${_t} PROPERTIES
                LABELS "plain" RUN_SERIAL TRUE RESOURCE_LOCK iotdb_cpp_it_server)
    endforeach()
    foreach(_t IN LISTS _it_ssl_examples)
        add_test(NAME example_${_t} COMMAND ${_t})
        set_tests_properties(example_${_t} PROPERTIES
                LABELS "ssl" RUN_SERIAL TRUE RESOURCE_LOCK iotdb_cpp_it_server)
    endforeach()
    foreach(_t IN LISTS _it_ntls_examples)
        add_test(NAME example_${_t} COMMAND ${_t})
        set_tests_properties(example_${_t} PROPERTIES
                LABELS "ntls" RUN_SERIAL TRUE RESOURCE_LOCK iotdb_cpp_it_server)
    endforeach()
endif()
