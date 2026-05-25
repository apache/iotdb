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

set(_test_targets
        session_tests
        session_relational_tests
        session_c_tests
        session_c_relational_tests)

add_executable(session_tests              main.cpp              cpp/sessionIT.cpp)
add_executable(session_relational_tests   main_Relational.cpp   cpp/sessionRelationalIT.cpp)
add_executable(session_c_tests            main_c.cpp            cpp/sessionCIT.cpp)
add_executable(session_c_relational_tests main_c_Relational.cpp cpp/sessionCRelationalIT.cpp)

foreach(_t IN LISTS _test_targets)
    target_include_directories(${_t} PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}/catch2"
            "${CMAKE_CURRENT_SOURCE_DIR}/../rpc"
            "${THRIFT_GEN_CPP_DIR}"
            "${THRIFT_INCLUDE_DIR}")
    if(BOOST_INCLUDE_DIR)
        target_include_directories(${_t} PRIVATE "${BOOST_INCLUDE_DIR}")
    endif()
    target_link_libraries(${_t} PRIVATE iotdb_session)
    if(WITH_SSL)
        target_link_libraries(${_t} PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    endif()
endforeach()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND NOT MSVC)
    foreach(_t IN LISTS _test_targets)
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
    foreach(_t IN LISTS _test_targets)
        add_custom_command(TARGET ${_t} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        $<TARGET_FILE:iotdb_session> $<TARGET_FILE_DIR:${_t}>)
    endforeach()
else()
    add_test(NAME sessionIT              COMMAND session_tests)
    add_test(NAME sessionRelationalIT    COMMAND session_relational_tests)
    add_test(NAME sessionCIT             COMMAND session_c_tests)
    add_test(NAME sessionCRelationalIT   COMMAND session_c_relational_tests)
endif()

# Run sequentially: parallel ctest overloads the single local IoTDB instance.
set_tests_properties(
        sessionIT sessionRelationalIT sessionCIT sessionCRelationalIT
        PROPERTIES RUN_SERIAL TRUE)
