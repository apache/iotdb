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
# IotdbResolveTarball.cmake
#
# Shared helper used by FetchBuildTools, FetchThrift, etc.
#
# Resolution order:
#   1. Exact filename under ${IOTDB_OS_DEPS_DIR}/
#   2. Optional GLOB_PATTERN match under ${IOTDB_OS_DEPS_DIR}/
#   3. file(DOWNLOAD) into ${IOTDB_OS_DEPS_DIR}/ when IOTDB_OFFLINE is OFF
#   4. FATAL_ERROR otherwise
# =============================================================================

function(_iotdb_resolve_tarball OUT_VAR FILENAME URL)
    cmake_parse_arguments(ARG "" "GLOB_PATTERN;LOG_PREFIX" "" ${ARGN})
    if(NOT ARG_LOG_PREFIX)
        set(ARG_LOG_PREFIX "Deps")
    endif()

    set(_local "${IOTDB_OS_DEPS_DIR}/${FILENAME}")
    if(EXISTS "${_local}")
        message(STATUS "[${ARG_LOG_PREFIX}] using ${_local}")
        set(${OUT_VAR} "${_local}" PARENT_SCOPE)
        return()
    endif()

    if(ARG_GLOB_PATTERN)
        file(GLOB _matches "${IOTDB_OS_DEPS_DIR}/${ARG_GLOB_PATTERN}")
        if(_matches)
            list(GET _matches 0 _hit)
            message(STATUS "[${ARG_LOG_PREFIX}] reusing ${_hit}")
            set(${OUT_VAR} "${_hit}" PARENT_SCOPE)
            return()
        endif()
    endif()

    if(IOTDB_OFFLINE)
        set(_hint "${FILENAME}")
        if(ARG_GLOB_PATTERN)
            set(_hint "${FILENAME} (or any ${ARG_GLOB_PATTERN})")
        endif()
        message(FATAL_ERROR
                "[${ARG_LOG_PREFIX}] IOTDB_OFFLINE=ON but ${_hint} is missing in "
                "${IOTDB_OS_DEPS_DIR}.")
    endif()

    message(STATUS "[${ARG_LOG_PREFIX}] downloading ${URL}")
    file(DOWNLOAD "${URL}" "${_local}" SHOW_PROGRESS STATUS _st TLS_VERIFY ON)
    list(GET _st 0 _code)
    if(NOT _code EQUAL 0)
        list(GET _st 1 _msg)
        file(REMOVE "${_local}")
        message(FATAL_ERROR "[${ARG_LOG_PREFIX}] download failed for ${FILENAME}: ${_msg}")
    endif()
    message(STATUS "[${ARG_LOG_PREFIX}] cached ${_local}")
    set(${OUT_VAR} "${_local}" PARENT_SCOPE)
endfunction()
