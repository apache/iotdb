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

project(iotdb_session_cpp)
cmake_minimum_required(VERSION 3.16)
set (CMAKE_REQUIRED_INCLUDES "/usr/local/include")
add_subdirectory(src)
add_subdirectory(target/generated-sources-cpp)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${CMAKE_SOURCE_DIR}/tools/thrift/target/thrift-0.13.0/lib/cpp/src")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")

aux_source_directory(./src SESSION_SRCS)

aux_source_directory(./target/generated-sources-cpp GEN_SRCS)

list(APPEND SESSION_SRCS ${GEN_SRCS})

link_directories(lib)

add_library(iotdb_session_cpp SHARED ${SESSION_SRCS})
#target_link_libraries(client-cpp Session)
#target_link_libraries(client-cpp GenCPP)

target_link_libraries(iotdb_session_cpp ${CMAKE_SOURCE_DIR}/tools/thrift/target/build/lib/libthrift.a)
