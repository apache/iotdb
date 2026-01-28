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

PROJECT(SessionExample)
CMAKE_MINIMUM_REQUIRED(VERSION 3.7)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Add Thrift include directory
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/thrift/include)
# Add cpp-client include directory
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/client/include)

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

FIND_PACKAGE(Boost REQUIRED)
IF (DEFINED BOOST_INCLUDEDIR)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
ENDIF()

# Add the libs for the cpp-client
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/client/lib)

ADD_EXECUTABLE(SessionExample SessionExample.cpp)
ADD_EXECUTABLE(AlignedTimeseriesSessionExample AlignedTimeseriesSessionExample.cpp)
ADD_EXECUTABLE(TableModelSessionExample TableModelSessionExample.cpp)
ADD_EXECUTABLE(MultiSvrNodeClient MultiSvrNodeClient.cpp)

IF(MSVC)
    IF(WITH_SSL)
        TARGET_LINK_LIBRARIES(SessionExample
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
            OpenSSL::SSL
            OpenSSL::Crypto
        )
        TARGET_LINK_LIBRARIES(AlignedTimeseriesSessionExample
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
            OpenSSL::SSL
            OpenSSL::Crypto
        )
        TARGET_LINK_LIBRARIES(TableModelSessionExample
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
            OpenSSL::SSL
            OpenSSL::Crypto
        )
        TARGET_LINK_LIBRARIES(MultiSvrNodeClient
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
            OpenSSL::SSL
            OpenSSL::Crypto
        )
    ELSE()
        TARGET_LINK_LIBRARIES(SessionExample
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
        )
        TARGET_LINK_LIBRARIES(AlignedTimeseriesSessionExample
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
        )
        TARGET_LINK_LIBRARIES(TableModelSessionExample
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
        )
        TARGET_LINK_LIBRARIES(MultiSvrNodeClient
            iotdb_session
            "${CMAKE_SOURCE_DIR}/thrift/lib/Release/thriftmd.lib"
        )
    ENDIF()
ELSE()
    IF(WITH_SSL)
        TARGET_LINK_LIBRARIES(SessionExample
            iotdb_session
            pthread
            OpenSSL::SSL
            OpenSSL::Crypto
        )
        TARGET_LINK_LIBRARIES(AlignedTimeseriesSessionExample
            iotdb_session
            pthread
            OpenSSL::SSL
            OpenSSL::Crypto
        )
        TARGET_LINK_LIBRARIES(TableModelSessionExample
            iotdb_session
            pthread
            OpenSSL::SSL
            OpenSSL::Crypto
        )
        TARGET_LINK_LIBRARIES(MultiSvrNodeClient
            iotdb_session
            pthread
            OpenSSL::SSL
            OpenSSL::Crypto
        )
    ELSE()
        TARGET_LINK_LIBRARIES(SessionExample
            iotdb_session
            pthread
        )
        TARGET_LINK_LIBRARIES(AlignedTimeseriesSessionExample
            iotdb_session
            pthread
        )
        TARGET_LINK_LIBRARIES(TableModelSessionExample
            iotdb_session
            pthread
        )
        TARGET_LINK_LIBRARIES(MultiSvrNodeClient
            iotdb_session
            pthread
        )
    ENDIF()
ENDIF()
