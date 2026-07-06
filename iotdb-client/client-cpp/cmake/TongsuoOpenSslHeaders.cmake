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
# =============================================================================
# Generate thin wrapper headers under openssl/ that include the bundled Tongsuo
# tree via absolute paths. macOS CI runners also ship OpenSSL-compatible headers
# in the Xcode SDK; angle-bracket includes can resolve there instead of Tongsuo
# and hide NTLS APIs even when -I points at the bundled install prefix.
# =============================================================================

function(iotdb_setup_tongsuo_openssl_headers _tongsuo_include_dir)
    if(NOT IS_DIRECTORY "${_tongsuo_include_dir}/openssl")
        message(FATAL_ERROR
                "[Tongsuo] expected headers under ${_tongsuo_include_dir}/openssl")
    endif()

    set(_wrap_dir "${CMAKE_BINARY_DIR}/generated/tongsuo-openssl-wrap")
    set(_ossl_wrap "${_wrap_dir}/openssl")
    file(MAKE_DIRECTORY "${_ossl_wrap}")
    set(_ossl_root "${_tongsuo_include_dir}/openssl")
    file(GLOB _ossl_headers RELATIVE "${_ossl_root}" "${_ossl_root}/*.h")
    foreach(_header ${_ossl_headers})
        file(WRITE "${_ossl_wrap}/${_header}"
                "#pragma once\n#include \"${_ossl_root}/${_header}\"\n")
    endforeach()

    if(NOT TARGET iotdb_tongsuo_openssl_wrap)
        add_library(iotdb_tongsuo_openssl_wrap INTERFACE)
        target_include_directories(iotdb_tongsuo_openssl_wrap BEFORE INTERFACE
                "${_wrap_dir}")
    endif()
    set(IOTDB_TONGSUO_OPENSSL_WRAP_DIR "${_wrap_dir}" PARENT_SCOPE)
endfunction()
