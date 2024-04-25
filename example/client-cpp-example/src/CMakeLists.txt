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
ADD_DEFINITIONS(-D FMT_HEADER_ONLY)
# Add Thrift include directory
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/thrift/include)
# Add cpp-client include directory
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/client/include)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../src/cpr/install/x64-Release/include)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../src/json)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../src/ThreadPool)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../src/fmt/include)

FIND_PACKAGE(Boost REQUIRED)
IF (DEFINED BOOST_INCLUDEDIR)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
ENDIF()

# Add the libs for the cpp-client
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/client/lib)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../src/cpr/install/x64-Release/lib)

# ADD_EXECUTABLE(SessionExample SessionExample.cpp)
ADD_EXECUTABLE (iotdbtest iotdbtest.cpp iotdbtest.h iotdbclient.h iotdbclient.cpp)

IF(MSVC)
    TARGET_LINK_LIBRARIES(iotdbtest PUBLIC
                                    debug iotdb_session optimized iotdb_session
                                    debug thriftmd optimized thriftmd
                                    )
ELSE()
    TARGET_LINK_LIBRARIES(iotdbtest iotdb_session pthread)
ENDIF()
