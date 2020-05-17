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

project(client-cpp)
cmake_minimum_required(VERSION 3.16)
add_subdirectory(Session)
add_subdirectory(gen-cpp)

aux_source_directory(./Session dir_Session_srcs)
add_library(Session ${dir_Session_srcs})

aux_source_directory(./gen-cpp dir_GenCPP_srcs)
add_library(GenCPP ${dir_GenCPP_srcs})

# if you already have thrift, just delete this code
include_directories(./thrift_0.13.0_src)

aux_source_directory(. EXE_SRCS)
add_executable(client-cpp ${EXE_SRCS})
target_link_libraries(client-cpp Session)
target_link_libraries(client-cpp GenCPP)

target_link_libraries(client-cpp libthrift.a)
target_link_libraries(client-cpp libthriftnb.a)
