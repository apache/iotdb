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
# C++ client integration tests (driven by ctest).
# The parent CMakeLists.txt include()s this folder when BUILD_TESTING=ON;
# it relies on the iotdb_session target (and iotdb_thrift_static) already
# being defined at the parent scope.
# =============================================================================

include(CTest)

set(CATCH2_URL
        "https://github.com/catchorg/Catch2/releases/download/v2.13.7/catch.hpp")
if(CATCH2_INCLUDE_DIR)
    set(_catch2_include_dir "${CATCH2_INCLUDE_DIR}")
else()
    set(_catch2_include_dir "${CMAKE_CURRENT_BINARY_DIR}/catch2")
endif()
set(_catch2_header "${_catch2_include_dir}/catch.hpp")
if(NOT EXISTS "${_catch2_header}")
    file(MAKE_DIRECTORY "${_catch2_include_dir}")
    message(STATUS "Downloading Catch2 from ${CATCH2_URL}")
    file(DOWNLOAD "${CATCH2_URL}" "${_catch2_header}" SHOW_PROGRESS TLS_VERIFY ON)
endif()

set(_plain_test_targets
        session_tests
        session_relational_tests
        session_c_tests
        session_c_relational_tests)

set(_rpc_ssl_test_targets rpc_ssl_utils_tests rpc_ntls_utils_tests)

set(_test_targets ${_plain_test_targets} ${_rpc_ssl_test_targets})

add_executable(session_tests              main.cpp              cpp/sessionIT.cpp)
add_executable(session_relational_tests   main_Relational.cpp   cpp/sessionRelationalIT.cpp)
add_executable(session_c_tests            main_c.cpp            cpp/sessionCIT.cpp)
add_executable(session_c_relational_tests main_c_Relational.cpp cpp/sessionCRelationalIT.cpp)
add_executable(rpc_ssl_utils_tests
        main_rpc_ssl.cpp
        cpp/RpcSslUtilsTest.cpp
        cpp/RpcSslTlsMutualAuthTest.cpp
        cpp/RpcSslIotdbE2eTest.cpp
        cpp/SslTestFixtures.cpp
        cpp/ItSslConnection.cpp)
add_executable(rpc_ntls_utils_tests
        main_rpc_ntls.cpp
        cpp/RpcSslTlcpMutualAuthTest.cpp
        cpp/RpcNtlsE2eTest.cpp
        cpp/SslTestFixtures.cpp)

foreach(_t IN LISTS _test_targets)
    target_include_directories(${_t} PRIVATE
            "${_catch2_include_dir}"
            "${CMAKE_CURRENT_SOURCE_DIR}/cpp"
            "${CMAKE_CURRENT_SOURCE_DIR}/../src/rpc"
            "${THRIFT_GEN_CPP_DIR}"
            "${THRIFT_INCLUDE_DIR}")
    if(BOOST_INCLUDE_DIR)
        target_include_directories(${_t} PRIVATE "${BOOST_INCLUDE_DIR}")
    endif()
    target_link_libraries(${_t} PRIVATE iotdb_session)
    if(WITH_SSL)
        target_link_libraries(${_t} PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        if(TARGET iotdb_tongsuo_openssl_wrap)
            target_link_libraries(${_t} PRIVATE iotdb_tongsuo_openssl_wrap)
        endif()
    endif()
endforeach()

# macOS: apply Tongsuo openssl header wrap to test targets (same as iotdb_session).
if(WITH_SSL AND TARGET iotdb_tongsuo_openssl_wrap)
    foreach(_t IN LISTS _test_targets)
        target_include_directories(${_t} BEFORE PRIVATE
                "${CMAKE_BINARY_DIR}/generated/tongsuo-openssl-wrap")
    endforeach()
endif()

if(WITH_SSL)
    target_compile_definitions(rpc_ssl_utils_tests PRIVATE IOTDB_RPC_SSL_IT=1)

    if(WIN32)
        set(_iotdb_openssl_executable "${OPENSSL_ROOT_DIR}/bin/openssl.exe")
    else()
        set(_iotdb_openssl_executable "${OPENSSL_ROOT_DIR}/bin/openssl")
    endif()
    file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/fixtures" _iotdb_test_fixtures_dir)
    file(TO_CMAKE_PATH "${_iotdb_openssl_executable}" _iotdb_openssl_executable_cmake)
    file(TO_CMAKE_PATH "${OPENSSL_ROOT_DIR}" _iotdb_openssl_root_dir)
    string(REPLACE "\\" "/" _iotdb_test_fixtures_dir_fwd "${_iotdb_test_fixtures_dir}")
    string(REPLACE "\\" "/" _iotdb_openssl_executable_fwd "${_iotdb_openssl_executable_cmake}")
    string(REPLACE "\\" "/" _iotdb_openssl_root_dir_fwd "${_iotdb_openssl_root_dir}")
    foreach(_t IN LISTS _rpc_ssl_test_targets)
        target_compile_definitions(${_t} PRIVATE
                IOTDB_TEST_FIXTURES_DIR="${_iotdb_test_fixtures_dir_fwd}")
        add_custom_command(TARGET ${_t} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_directory
                        "${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
                        "$<TARGET_FILE_DIR:${_t}>/fixtures"
                COMMENT "Copy SSL test fixtures next to ${_t}")
    endforeach()
    target_compile_definitions(rpc_ssl_utils_tests PRIVATE
            IOTDB_OPENSSL_EXECUTABLE="${_iotdb_openssl_executable_fwd}"
            IOTDB_OPENSSL_ROOT_DIR="${_iotdb_openssl_root_dir_fwd}")
    target_compile_definitions(rpc_ntls_utils_tests PRIVATE
            IOTDB_OPENSSL_EXECUTABLE="${_iotdb_openssl_executable_fwd}"
            IOTDB_OPENSSL_ROOT_DIR="${_iotdb_openssl_root_dir_fwd}")
    if(WIN32)
        foreach(_t IN LISTS _rpc_ssl_test_targets)
            target_link_libraries(${_t} PRIVATE ws2_32 "${THRIFT_STATIC_LIB_PATH}")
        endforeach()
    else()
        foreach(_t IN LISTS _rpc_ssl_test_targets)
            target_link_libraries(${_t} PRIVATE iotdb_thrift_static)
        endforeach()
    endif()
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND NOT MSVC AND NOT APPLE)
    set(_iotdb_asan_targets session_tests session_relational_tests session_c_tests
            session_c_relational_tests rpc_ssl_utils_tests rpc_ntls_utils_tests)
    foreach(_t IN LISTS _iotdb_asan_targets)
        target_compile_options(${_t} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(${_t} PRIVATE -fsanitize=address)
    endforeach()
endif()

# Linux: keep iotdb_session in the executable's needed-list even when there
# is no direct symbol reference from main.cpp (the IT helpers pull it in
# transitively); without this, ld may drop the lib at link time.
if(UNIX AND NOT APPLE)
    foreach(_t IN LISTS _test_targets)
        target_link_options(${_t} PRIVATE -Wl,--no-as-needed)
    endforeach()
endif()

# Register ctest cases (multi-config on MSVC).
if(MSVC)
    add_test(NAME sessionIT              CONFIGURATIONS Release COMMAND session_tests)
    add_test(NAME sessionRelationalIT    CONFIGURATIONS Release COMMAND session_relational_tests)
    add_test(NAME sessionCIT             CONFIGURATIONS Release COMMAND session_c_tests)
    add_test(NAME sessionCRelationalIT   CONFIGURATIONS Release COMMAND session_c_relational_tests)
    add_test(NAME rpcSslUtilsTest      CONFIGURATIONS Release COMMAND rpc_ssl_utils_tests)
    add_test(NAME rpcNtlsUtilsTest     CONFIGURATIONS Release COMMAND rpc_ntls_utils_tests)
    foreach(_t IN LISTS _test_targets)
        add_custom_command(TARGET ${_t} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        $<TARGET_FILE:iotdb_session> $<TARGET_FILE_DIR:${_t}>)
        if(WITH_SSL AND ${_t} IN_LIST _rpc_ssl_test_targets)
            _iotdb_collect_openssl_windows_dlls(_iotdb_ssl_runtime_dlls)
            foreach(_ssl_dll IN LISTS _iotdb_ssl_runtime_dlls)
                add_custom_command(TARGET ${_t} POST_BUILD
                        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                                "${_ssl_dll}" $<TARGET_FILE_DIR:${_t}>
                        COMMENT "Copy bundled SSL runtime next to ${_t}")
            endforeach()
        endif()
    endforeach()
else()
    add_test(NAME sessionIT              COMMAND session_tests)
    add_test(NAME sessionRelationalIT    COMMAND session_relational_tests)
    add_test(NAME sessionCIT             COMMAND session_c_tests)
    add_test(NAME sessionCRelationalIT   COMMAND session_c_relational_tests)
    add_test(NAME rpcSslUtilsTest      COMMAND rpc_ssl_utils_tests)
    add_test(NAME rpcNtlsUtilsTest     COMMAND rpc_ntls_utils_tests)
    foreach(_t IN LISTS _test_targets)
        add_custom_command(TARGET ${_t} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        $<TARGET_FILE:iotdb_session> $<TARGET_FILE_DIR:${_t}>
                COMMENT "Copy IoTDB runtime library next to ${_t}")
        if(WITH_SSL AND ${_t} IN_LIST _rpc_ssl_test_targets)
            foreach(_ssl_lib IN LISTS OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY)
                if(_ssl_lib AND EXISTS "${_ssl_lib}")
                    add_custom_command(TARGET ${_t} POST_BUILD
                            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                                    "${_ssl_lib}" $<TARGET_FILE_DIR:${_t}>
                            COMMENT "Copy bundled SSL runtime next to ${_t}")
                endif()
            endforeach()
        endif()
    endforeach()
endif()

set_tests_properties(
        sessionIT sessionRelationalIT sessionCIT sessionCRelationalIT
        PROPERTIES LABELS "plain" RUN_SERIAL TRUE RESOURCE_LOCK iotdb_cpp_it_server)
set_tests_properties(
        rpcSslUtilsTest
        PROPERTIES LABELS "ssl" RUN_SERIAL TRUE RESOURCE_LOCK iotdb_cpp_it_server)
set_tests_properties(
        rpcNtlsUtilsTest
        PROPERTIES LABELS "ntls" RUN_SERIAL TRUE RESOURCE_LOCK iotdb_cpp_it_server)
