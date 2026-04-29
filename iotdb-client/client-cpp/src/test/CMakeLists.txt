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

CMAKE_MINIMUM_REQUIRED(VERSION 3.7)
INCLUDE( CTest )
SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(TARGET_NAME session_tests)
SET(TARGET_NAME_RELATIONAL session_relational_tests)
IF(NOT MSVC)
    # Keep GCC/Clang style flags off on MSVC to avoid invalid-option build errors.
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O2")
ENDIF()
ENABLE_TESTING()

# =========================
# SSL option (default OFF)
# =========================
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

# Add Boost include path for MacOS
INCLUDE_DIRECTORIES(/usr/local/include)
# Add Session related include files
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main/generated-sources-cpp)
# Add Thrift include directory
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../../thrift/include)

# Add Boost library headers for MaxOS
FIND_PACKAGE(Boost REQUIRED)
IF (DEFINED BOOST_INCLUDEDIR)
    include_directories("${Boost_INCLUDE_DIR}")
ENDIF()

# Link directories are different for Windows and Linux/Mac
IF(MSVC)
    LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main/Release)
    SET(THRIFT_STATIC_LIB "${CMAKE_SOURCE_DIR}/../../thrift/lib/Release/thriftmd.lib")
ELSE()
    LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main)
    SET(THRIFT_STATIC_LIB "${CMAKE_SOURCE_DIR}/../../thrift/lib/libthrift.a")
ENDIF()

IF(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND NOT MSVC)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
ENDIF()

SET(TARGET_NAME_C session_c_tests)
SET(TARGET_NAME_C_RELATIONAL session_c_relational_tests)

ADD_EXECUTABLE(${TARGET_NAME} main.cpp cpp/sessionIT.cpp)
ADD_EXECUTABLE(${TARGET_NAME_RELATIONAL} main_Relational.cpp cpp/sessionRelationalIT.cpp)
ADD_EXECUTABLE(${TARGET_NAME_C} main_c.cpp cpp/sessionCIT.cpp)
ADD_EXECUTABLE(${TARGET_NAME_C_RELATIONAL} main_c_Relational.cpp cpp/sessionCRelationalIT.cpp)

# Link with shared library iotdb_session and pthread
SET(ALL_TEST_TARGETS ${TARGET_NAME} ${TARGET_NAME_RELATIONAL} ${TARGET_NAME_C} ${TARGET_NAME_C_RELATIONAL})

IF(MSVC)
    IF(WITH_SSL)
        FOREACH(T ${ALL_TEST_TARGETS})
            TARGET_LINK_LIBRARIES(${T}
                iotdb_session
                ${THRIFT_STATIC_LIB}
                OpenSSL::SSL
                OpenSSL::Crypto
            )
        ENDFOREACH()
    ELSE()
        FOREACH(T ${ALL_TEST_TARGETS})
            TARGET_LINK_LIBRARIES(${T}
                iotdb_session
                ${THRIFT_STATIC_LIB}
            )
        ENDFOREACH()
    ENDIF()
ELSE()
    IF(WITH_SSL)
        FOREACH(T ${ALL_TEST_TARGETS})
            TARGET_LINK_LIBRARIES(${T}
                iotdb_session
                pthread
                OpenSSL::SSL
                OpenSSL::Crypto
            )
        ENDFOREACH()
    ELSE()
        FOREACH(T ${ALL_TEST_TARGETS})
            TARGET_LINK_LIBRARIES(${T}
                iotdb_session
                pthread
            )
        ENDFOREACH()
    ENDIF()
ENDIF()

# Add Catch2 include directory
FOREACH(T ${ALL_TEST_TARGETS})
    TARGET_INCLUDE_DIRECTORIES(${T} PUBLIC ./catch2/)
ENDFOREACH()

# Add tests to ctest
IF(MSVC)
    ADD_TEST(NAME sessionIT CONFIGURATIONS Release COMMAND ${TARGET_NAME})
    ADD_TEST(NAME sessionRelationalIT CONFIGURATIONS Release COMMAND ${TARGET_NAME_RELATIONAL})
    ADD_TEST(NAME sessionCIT CONFIGURATIONS Release COMMAND ${TARGET_NAME_C})
    ADD_TEST(NAME sessionCRelationalIT CONFIGURATIONS Release COMMAND ${TARGET_NAME_C_RELATIONAL})
ELSE()
    ADD_TEST(NAME sessionIT COMMAND ${TARGET_NAME})
    ADD_TEST(NAME sessionRelationalIT COMMAND ${TARGET_NAME_RELATIONAL})
    ADD_TEST(NAME sessionCIT COMMAND ${TARGET_NAME_C})
    ADD_TEST(NAME sessionCRelationalIT COMMAND ${TARGET_NAME_C_RELATIONAL})
ENDIF()

# One process at a time: parallel ctest overloads a single local IoTDB on 127.0.0.1:6667.
SET_TESTS_PROPERTIES(sessionIT sessionRelationalIT sessionCIT sessionCRelationalIT PROPERTIES RUN_SERIAL TRUE)

if(UNIX AND NOT APPLE)
  target_link_options(session_tests PRIVATE -Wl,--no-as-needed)
  target_link_options(session_relational_tests PRIVATE -Wl,--no-as-needed)
  target_link_options(session_c_tests PRIVATE -Wl,--no-as-needed)
  target_link_options(session_c_relational_tests PRIVATE -Wl,--no-as-needed)
endif()
