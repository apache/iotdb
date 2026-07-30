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

if(NOT IOTDB_SESSION_LIBRARY)
    message(FATAL_ERROR "IOTDB_SESSION_LIBRARY is required")
endif()

find_program(IOTDB_NM_EXECUTABLE nm)
if(NOT IOTDB_NM_EXECUTABLE)
    message(FATAL_ERROR "nm is required to verify _GLIBCXX_USE_CXX11_ABI=1")
endif()

execute_process(
        COMMAND "${IOTDB_NM_EXECUTABLE}" -D --demangle "${IOTDB_SESSION_LIBRARY}"
        OUTPUT_VARIABLE _iotdb_session_symbols
        ERROR_VARIABLE _iotdb_nm_error
        RESULT_VARIABLE _iotdb_nm_result)
if(NOT _iotdb_nm_result EQUAL 0)
    message(FATAL_ERROR
            "Failed to inspect ${IOTDB_SESSION_LIBRARY}: ${_iotdb_nm_error}")
endif()

string(FIND
        "${_iotdb_session_symbols}"
        "Session::setStorageGroup(std::__cxx11::basic_string"
        _iotdb_session_cxx11_symbol)
string(FIND
        "${_iotdb_session_symbols}"
        "SessionDataSet::getColumnNames[abi:cxx11]() const"
        _iotdb_dataset_cxx11_symbol)
string(FIND
        "${_iotdb_session_symbols}"
        "RowRecord::toString[abi:cxx11]()"
        _iotdb_row_record_cxx11_symbol)

if(_iotdb_session_cxx11_symbol EQUAL -1
        OR _iotdb_dataset_cxx11_symbol EQUAL -1
        OR _iotdb_row_record_cxx11_symbol EQUAL -1)
    message(FATAL_ERROR
            "${IOTDB_SESSION_LIBRARY} is not built with "
            "_GLIBCXX_USE_CXX11_ABI=1 for IoTDB C++ API symbols.")
endif()

message(STATUS
        "${IOTDB_SESSION_LIBRARY} passed _GLIBCXX_USE_CXX11_ABI=1 symbol check")
