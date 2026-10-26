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
add_executable(odbc_unit unit.cpp)
target_link_libraries(odbc_unit PRIVATE odbc_objects)
if(WIN32)
    target_sources(odbc_unit PRIVATE ../src/setup/setup.rc)
endif()
add_test(NAME odbc_unit COMMAND odbc_unit)
if(NOT WIN32)
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/odbc.ini"
        "[ODBC_TLS_TEST]\nSERVER=localhost\nSSL=1\nSSLCA=dsn-ca.pem\nSSLCERT=dsn-client.pem\nSSLKEY=dsn-client.key\n")
    set_tests_properties(odbc_unit PROPERTIES
        ENVIRONMENT "ODBCINI=${CMAKE_CURRENT_BINARY_DIR}/odbc.ini")
endif()

add_executable(odbc_manager manager.cpp)
if(WIN32)
    target_link_libraries(odbc_manager PRIVATE odbc32 advapi32)
else()
    find_library(ODBC_MANAGER_LIBRARY odbc REQUIRED)
    target_include_directories(odbc_manager PRIVATE "${ODBC_INCLUDE_DIR}")
    target_link_libraries(odbc_manager PRIVATE "${ODBC_MANAGER_LIBRARY}")
    target_link_options(odbc_manager PRIVATE -static-libstdc++ -static-libgcc)
endif()
add_test(NAME odbc_manager_load COMMAND odbc_manager $<TARGET_FILE:apache_iotdb_odbc>)
set_tests_properties(odbc_unit odbc_manager_load PROPERTIES TIMEOUT 30)
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
    add_test(NAME odbc_parameters COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/parameters.py" $<TARGET_FILE:apache_iotdb_odbc>)
    set_tests_properties(odbc_parameters PROPERTIES TIMEOUT 30)
    add_test(NAME odbc_compatibility COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/compatibility.py" $<TARGET_FILE:apache_iotdb_odbc>)
    set_tests_properties(odbc_compatibility PROPERTIES TIMEOUT 30)
endif()
install(TARGETS odbc_manager RUNTIME DESTINATION test COMPONENT Tests)
