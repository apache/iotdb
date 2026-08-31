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
# FetchBuildTools.cmake
#
# Apache Thrift's source build needs a working flex / bison toolchain (m4 too
# on Unix). When the host already provides them on PATH we use them as-is.
# Otherwise we provision them locally:
#
#   * Linux / macOS - configure-make-install each tool from a tarball into
#     ${CMAKE_BINARY_DIR}/tools (no sudo required).
#   * Windows       - extract the winflexbison zip and copy
#     win_flex.exe -> flex.exe, win_bison.exe -> bison.exe.
#
# Tarballs / zips are resolved with the standard three-stage pattern:
#   1. ${IOTDB_OS_DEPS_DIR}/<filename> (or any match for an optional GLOB)
#   2. file(DOWNLOAD) when IOTDB_OFFLINE is OFF
#   3. FATAL_ERROR otherwise
# =============================================================================

set(_tools_prefix "${CMAKE_BINARY_DIR}/tools")
set(_tools_bin "${_tools_prefix}/bin")
file(MAKE_DIRECTORY "${_tools_bin}")

# Make sure any tool we install locally takes precedence over the system PATH
# for the remainder of the configure step (and child ExternalProject calls).
if(WIN32)
    set(ENV{PATH} "${_tools_bin};$ENV{PATH}")
else()
    set(ENV{PATH} "${_tools_bin}:$ENV{PATH}")
endif()

set(M4_VERSION    "1.4.19" CACHE STRING "GNU m4 version to build when missing")
set(FLEX_VERSION  "2.6.4"  CACHE STRING "GNU flex version to build when missing")
set(BISON_VERSION "3.8"    CACHE STRING "GNU bison version to build when missing")
set(WINFLEXBISON_VERSION "2.5.25"
        CACHE STRING "winflexbison version to download when no local zip is present")

set(_m4_url    "https://ftp.gnu.org/gnu/m4/m4-${M4_VERSION}.tar.gz")
set(_flex_url  "https://github.com/westes/flex/releases/download/v${FLEX_VERSION}/flex-${FLEX_VERSION}.tar.gz")
set(_bison_url "https://ftp.gnu.org/gnu/bison/bison-${BISON_VERSION}.tar.gz")
set(_winflexbison_url
        "https://github.com/lexxmark/winflexbison/releases/download/v${WINFLEXBISON_VERSION}/win_flex_bison-${WINFLEXBISON_VERSION}.zip")
set(_winflexbison_filename "win_flex_bison-${WINFLEXBISON_VERSION}.zip")

include(ProcessorCount)
ProcessorCount(_jobs)
if(_jobs LESS 1)
    set(_jobs 1)
endif()

# Resolve tarball: prefer the exact filename in ${IOTDB_OS_DEPS_DIR}/, then
# any path matching GLOB_PATTERN (caller-supplied wildcard for relaxed naming,
# e.g. win_flex_bison*.zip), and finally fall back to a download.
function(_iotdb_resolve_tarball OUT_VAR FILENAME URL)
    cmake_parse_arguments(ARG "" "GLOB_PATTERN" "" ${ARGN})

    set(_local "${IOTDB_OS_DEPS_DIR}/${FILENAME}")
    if(EXISTS "${_local}")
        set(${OUT_VAR} "${_local}" PARENT_SCOPE)
        return()
    endif()

    if(ARG_GLOB_PATTERN)
        file(GLOB _matches "${IOTDB_OS_DEPS_DIR}/${ARG_GLOB_PATTERN}")
        if(_matches)
            list(GET _matches 0 _hit)
            message(STATUS "[BuildTools] reusing ${_hit}")
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
                "[BuildTools] IOTDB_OFFLINE=ON but ${_hint} is missing in "
                "${IOTDB_OS_DEPS_DIR}.")
    endif()

    message(STATUS "[BuildTools] downloading ${URL}")
    file(DOWNLOAD "${URL}" "${_local}" SHOW_PROGRESS STATUS _st TLS_VERIFY ON)
    list(GET _st 0 _code)
    if(NOT _code EQUAL 0)
        list(GET _st 1 _msg)
        file(REMOVE "${_local}")
        message(FATAL_ERROR "[BuildTools] download failed for ${FILENAME}: ${_msg}")
    endif()
    set(${OUT_VAR} "${_local}" PARENT_SCOPE)
endfunction()

# Configure-make-install <name> from <tarball> into ${_tools_prefix}.
function(_iotdb_build_autotools NAME TARBALL EXTRACTED_DIRNAME)
    set(_src_root "${CMAKE_BINARY_DIR}/_deps/${NAME}")
    set(_marker "${_tools_prefix}/.${NAME}-installed")
    if(EXISTS "${_marker}")
        return()
    endif()
    file(REMOVE_RECURSE "${_src_root}")
    file(MAKE_DIRECTORY "${_src_root}")
    message(STATUS "[BuildTools] extracting ${TARBALL}")
    file(ARCHIVE_EXTRACT INPUT "${TARBALL}" DESTINATION "${_src_root}")
    set(_src "${_src_root}/${EXTRACTED_DIRNAME}")
    if(NOT EXISTS "${_src}/configure")
        message(FATAL_ERROR
                "[BuildTools] expected configure script at ${_src}/configure")
    endif()
    message(STATUS "[BuildTools] building ${NAME} -> ${_tools_prefix}")
    # flex 2.6.4: reallocarray() needs _GNU_SOURCE on glibc 2.26+ (westes/flex#241).
    set(_env_prefix "")
    if(NAME STREQUAL "flex" AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
        set(_env_prefix env CFLAGS=-D_GNU_SOURCE CXXFLAGS=-D_GNU_SOURCE)
    endif()
    if(_env_prefix)
        execute_process(
                COMMAND ${_env_prefix} ./configure --prefix=${_tools_prefix}
                WORKING_DIRECTORY "${_src}"
                RESULT_VARIABLE _rc)
    else()
        execute_process(
                COMMAND ./configure --prefix=${_tools_prefix}
                WORKING_DIRECTORY "${_src}"
                RESULT_VARIABLE _rc)
    endif()
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[BuildTools] configure failed for ${NAME}")
    endif()
    if(_env_prefix)
        execute_process(
                COMMAND ${_env_prefix} make -j${_jobs}
                WORKING_DIRECTORY "${_src}"
                RESULT_VARIABLE _rc)
    else()
        execute_process(
                COMMAND make -j${_jobs}
                WORKING_DIRECTORY "${_src}"
                RESULT_VARIABLE _rc)
    endif()
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[BuildTools] make failed for ${NAME}")
    endif()
    if(_env_prefix)
        execute_process(
                COMMAND ${_env_prefix} make install
                WORKING_DIRECTORY "${_src}"
                RESULT_VARIABLE _rc)
    else()
        execute_process(
                COMMAND make install
                WORKING_DIRECTORY "${_src}"
                RESULT_VARIABLE _rc)
    endif()
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[BuildTools] make install failed for ${NAME}")
    endif()
    file(TOUCH "${_marker}")
endfunction()

# =============================================================================
# Windows branch - winflexbison
# =============================================================================
if(WIN32)
    # Stage 1: pick up an existing flex/bison if the host already provides it.
    find_program(FLEX_EXECUTABLE  NAMES flex  win_flex)
    find_program(BISON_EXECUTABLE NAMES bison win_bison)

    if(FLEX_EXECUTABLE AND BISON_EXECUTABLE)
        message(STATUS "[BuildTools] using system flex  = ${FLEX_EXECUTABLE}")
        message(STATUS "[BuildTools] using system bison = ${BISON_EXECUTABLE}")
        set(IOTDB_LOCAL_TOOLS_BIN "${_tools_bin}" CACHE INTERNAL "")
        return()
    endif()

    # Stage 2/3: resolve and extract the winflexbison zip into _tools_bin.
    _iotdb_resolve_tarball(_wfb_zip
            "${_winflexbison_filename}"
            "${_winflexbison_url}"
            GLOB_PATTERN "win_flex_bison*.zip")

    set(_wfb_marker "${_tools_bin}/.winflexbison-installed")
    if(NOT EXISTS "${_wfb_marker}")
        message(STATUS "[BuildTools] extracting ${_wfb_zip}")
        file(ARCHIVE_EXTRACT INPUT "${_wfb_zip}" DESTINATION "${_tools_bin}")

        if(NOT EXISTS "${_tools_bin}/win_flex.exe" OR NOT EXISTS "${_tools_bin}/win_bison.exe")
            message(FATAL_ERROR
                    "[BuildTools] win_flex.exe / win_bison.exe not found after "
                    "extracting ${_wfb_zip} into ${_tools_bin}.")
        endif()

        # Copy with renamed targets so thrift's CMakeLists sees flex/bison.
        execute_process(COMMAND ${CMAKE_COMMAND} -E copy
                "${_tools_bin}/win_flex.exe"  "${_tools_bin}/flex.exe"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[BuildTools] failed to copy win_flex.exe -> flex.exe")
        endif()
        execute_process(COMMAND ${CMAKE_COMMAND} -E copy
                "${_tools_bin}/win_bison.exe" "${_tools_bin}/bison.exe"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[BuildTools] failed to copy win_bison.exe -> bison.exe")
        endif()
        file(TOUCH "${_wfb_marker}")
    endif()

    find_program(FLEX_EXECUTABLE  flex  PATHS "${_tools_bin}" NO_DEFAULT_PATH REQUIRED)
    find_program(BISON_EXECUTABLE bison PATHS "${_tools_bin}" NO_DEFAULT_PATH REQUIRED)

    message(STATUS "[BuildTools] flex  = ${FLEX_EXECUTABLE}")
    message(STATUS "[BuildTools] bison = ${BISON_EXECUTABLE}")
    set(IOTDB_LOCAL_TOOLS_BIN "${_tools_bin}" CACHE INTERNAL "")
    return()
endif()

# =============================================================================
# Linux / macOS branch - m4 / flex / bison from autotools tarballs
# =============================================================================

# m4 (flex/bison both depend on this)
find_program(M4_EXECUTABLE m4)
if(NOT M4_EXECUTABLE)
    _iotdb_resolve_tarball(_m4_tarball "m4-${M4_VERSION}.tar.gz" "${_m4_url}")
    _iotdb_build_autotools(m4 "${_m4_tarball}" "m4-${M4_VERSION}")
    find_program(M4_EXECUTABLE m4 PATHS "${_tools_bin}" NO_DEFAULT_PATH REQUIRED)
endif()
message(STATUS "[BuildTools] m4    = ${M4_EXECUTABLE}")

# flex
find_program(FLEX_EXECUTABLE flex)
if(NOT FLEX_EXECUTABLE)
    _iotdb_resolve_tarball(_flex_tarball "flex-${FLEX_VERSION}.tar.gz" "${_flex_url}")
    _iotdb_build_autotools(flex "${_flex_tarball}" "flex-${FLEX_VERSION}")
    find_program(FLEX_EXECUTABLE flex PATHS "${_tools_bin}" NO_DEFAULT_PATH REQUIRED)
endif()
message(STATUS "[BuildTools] flex  = ${FLEX_EXECUTABLE}")

# bison - Thrift 0.23's grammar build uses bison >= 3.7 features (e.g. the
# --file-prefix-map option), so reject an older system bison (manylinux_2_28
# ships 3.0.4) and build ${BISON_VERSION} from source instead.
set(_bison_min_version "3.7")
find_program(BISON_EXECUTABLE bison)
if(BISON_EXECUTABLE)
    execute_process(COMMAND "${BISON_EXECUTABLE}" --version
            OUTPUT_VARIABLE _bison_ver_out ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REGEX MATCH "[0-9]+\\.[0-9]+(\\.[0-9]+)?" _bison_ver "${_bison_ver_out}")
    if(_bison_ver AND _bison_ver VERSION_LESS _bison_min_version)
        message(STATUS
                "[BuildTools] system bison ${_bison_ver} < ${_bison_min_version} "
                "(too old for Thrift ${THRIFT_VERSION}); building ${BISON_VERSION} from source")
        unset(BISON_EXECUTABLE CACHE)
    endif()
endif()
if(NOT BISON_EXECUTABLE)
    _iotdb_resolve_tarball(_bison_tarball "bison-${BISON_VERSION}.tar.gz" "${_bison_url}")
    _iotdb_build_autotools(bison "${_bison_tarball}" "bison-${BISON_VERSION}")
    find_program(BISON_EXECUTABLE bison PATHS "${_tools_bin}" NO_DEFAULT_PATH REQUIRED)
endif()
message(STATUS "[BuildTools] bison = ${BISON_EXECUTABLE}")

# Expose the bin dir for downstream ExternalProject_Add calls.
set(IOTDB_LOCAL_TOOLS_BIN "${_tools_bin}" CACHE INTERNAL "")
