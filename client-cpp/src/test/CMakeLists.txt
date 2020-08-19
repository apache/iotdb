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
SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(TARGET_NAME tests)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall")
SET(TOOLS_DIR "${CMAKE_SOURCE_DIR}/../../../../compile-tools")
ENABLE_TESTING()

IF(MSVC)
    INCLUDE_DIRECTORIES(${TOOLS_DIR}/boost/target/lib/boost/include/boost-1_72)
ELSE()
    INCLUDE_DIRECTORIES(${TOOLS_DIR}/boost/target/lib/boost/include)
ENDIF()

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main/generated-sources-cpp)
INCLUDE_DIRECTORIES(${TOOLS_DIR}/thrift/target/thrift-0.13.0/lib/cpp/src)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../main)

ADD_EXECUTABLE(${TARGET_NAME} main.cpp cpp/sessionIT.cpp)
TARGET_LINK_LIBRARIES(${TARGET_NAME} PUBLIC iotdb_session)
TARGET_INCLUDE_DIRECTORIES(${TARGET_NAME} PUBLIC ./catch2/)

ADD_TEST(NAME test1 COMMAND ${TARGET_NAME})