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

set(_test_targets
        session_tests
        session_c_tests)

add_executable(session_tests   main.cpp   cpp/sessionIT.cpp)
add_executable(session_c_tests main_c.cpp cpp/sessionCIT.cpp)

foreach(_t IN LISTS _test_targets)
    target_include_directories(${_t} PRIVATE
            "${_catch2_include_dir}"
            "${CMAKE_CURRENT_SOURCE_DIR}/../src/rpc"
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
    add_test(NAME sessionIT   CONFIGURATIONS Release COMMAND session_tests)
    add_test(NAME sessionCIT  CONFIGURATIONS Release COMMAND session_c_tests)
    foreach(_t IN LISTS _test_targets)
        add_custom_command(TARGET ${_t} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        $<TARGET_FILE:iotdb_session> $<TARGET_FILE_DIR:${_t}>)
    endforeach()
else()
    add_test(NAME sessionIT   COMMAND session_tests)
    add_test(NAME sessionCIT  COMMAND session_c_tests)
endif()

# Run sequentially: parallel ctest overloads the single local IoTDB instance.
set_tests_properties(sessionIT sessionCIT PROPERTIES RUN_SERIAL TRUE)
