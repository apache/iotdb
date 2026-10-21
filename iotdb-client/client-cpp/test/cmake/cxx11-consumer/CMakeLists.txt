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
cmake_minimum_required(VERSION 3.16)
project(iotdb_cxx11_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package(iotdb-session REQUIRED CONFIG)

add_executable(cxx11_consumer main.cpp)
target_link_libraries(cxx11_consumer PRIVATE IoTDB::iotdb_session)
if(WIN32)
    get_target_property(_sdk_runtime IoTDB::iotdb_session IMPORTED_LOCATION)
    get_filename_component(_sdk_lib_dir "${_sdk_runtime}" DIRECTORY)
    file(GLOB _sdk_dlls "${_sdk_lib_dir}/*.dll")
    foreach(_dll IN LISTS _sdk_dlls)
        add_custom_command(TARGET cxx11_consumer POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        "${_dll}" $<TARGET_FILE_DIR:cxx11_consumer>)
    endforeach()
endif()

enable_testing()
add_test(NAME cxx11_consumer COMMAND cxx11_consumer)
