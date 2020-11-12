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
PROJECT(iotdb_session CXX)
SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -Wall")
SET(TOOLS_DIR "${CMAKE_SOURCE_DIR}/../../../../compile-tools")

# TODO May use FIND_PACKAGE instead
IF(MSVC)
    SET(THRIFT_STATIC_LIB "${TOOLS_DIR}/thrift/target/build/lib/Release/thriftmd.lib")
    INCLUDE_DIRECTORIES(${TOOLS_DIR}/boost/target/lib/boost/include/boost-1_72)
ELSE()
    SET(THRIFT_STATIC_LIB "${TOOLS_DIR}/thrift/target/build/lib/libthrift.a")
    INCLUDE_DIRECTORIES(${TOOLS_DIR}/boost/target/lib/boost/include)
ENDIF()

INCLUDE_DIRECTORIES(${TOOLS_DIR}/thrift/target/thrift-0.13.0/lib/cpp/src)
#ADD_SUBDIRECTORY(generated-sources-cpp)

AUX_SOURCE_DIRECTORY(./generated-sources-cpp SESSION_SRCS)

ADD_LIBRARY(iotdb_session SHARED ${SESSION_SRCS})
TARGET_LINK_LIBRARIES(iotdb_session ${THRIFT_STATIC_LIB})
