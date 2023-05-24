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
SET(TOOLS_DIR "${CMAKE_SOURCE_DIR}/../../../../compile-tools")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -g -O2")
ENABLE_TESTING()

# Add Boost include path for MacOS
INCLUDE_DIRECTORIES(/usr/local/include)
# Add Session related include files
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main/generated-sources-cpp)
# Add Thrift include directory
INCLUDE_DIRECTORIES(${TOOLS_DIR}/thrift/target/thrift-0.14.1/lib/cpp/src)

find_package(Boost REQUIRED)
IF (DEFINED BOOST_INCLUDEDIR)
    include_directories("${Boost_INCLUDE_DIR}")
ENDIF()

# Link directories are different for Windows and Linux/Mac
IF(MSVC)
    LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main/Release)
    SET(THRIFT_STATIC_LIB "${TOOLS_DIR}/thrift/target/build/lib/Release/thriftmd.lib")
ELSE()
    LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main)
ENDIF()

ADD_EXECUTABLE(${TARGET_NAME} main.cpp cpp/sessionIT.cpp)

# Link with shared library iotdb_session and pthread
IF(MSVC)
    TARGET_LINK_LIBRARIES(${TARGET_NAME} iotdb_session ${THRIFT_STATIC_LIB})
ELSE()
    TARGET_LINK_LIBRARIES(${TARGET_NAME} iotdb_session pthread)
ENDIF()
TARGET_INCLUDE_DIRECTORIES(${TARGET_NAME} PUBLIC ./catch2/)

# Add 'sessionIT' to the project to be run by ctest
IF(MSVC)
    ADD_TEST(NAME sessionIT CONFIGURATIONS Release COMMAND ${TARGET_NAME})
ELSE()
    ADD_TEST(NAME sessionIT COMMAND ${TARGET_NAME})
ENDIF()
