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
# FetchBoost.cmake
#
# Resolves the location of Boost headers needed at thrift / iotdb_session
# compile time, in three stages:
#
#   1. Use system / user-provided Boost (find_package, BOOST_ROOT, etc.).
#   2. Fall back to a local tarball under ${IOTDB_OS_DEPS_DIR}/.
#   3. Otherwise download the tarball from archives.boost.io (unless
#      IOTDB_OFFLINE is ON).
#
# Output (cache) variables:
#   BOOST_INCLUDE_DIR - directory containing <boost/...> headers.
# =============================================================================

if(DEFINED BOOST_INCLUDE_DIR AND EXISTS "${BOOST_INCLUDE_DIR}/boost/version.hpp")
    message(STATUS "[Boost] reusing cached BOOST_INCLUDE_DIR=${BOOST_INCLUDE_DIR}")
    return()
endif()

# ---------------------------------------------------------------------------
# Stage 1: find_package(Boost) - respects BOOST_ROOT / Boost_INCLUDE_DIR
# ---------------------------------------------------------------------------
# CMP0167 (CMake 3.30+): the bundled FindBoost was removed in favour of
# BoostConfig.cmake shipped by Boost itself. For broad compatibility (we may
# be looking at a Boost source tree that does NOT ship BoostConfig), keep the
# legacy FindBoost behaviour and silence the developer warning.
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 OLD)
endif()

find_package(Boost QUIET)
if(Boost_FOUND AND Boost_INCLUDE_DIRS)
    set(BOOST_INCLUDE_DIR "${Boost_INCLUDE_DIRS}" CACHE PATH "Boost include directory" FORCE)
    message(STATUS "[Boost] using system Boost at ${BOOST_INCLUDE_DIR}")
    return()
endif()

# Allow plain -DBOOST_INCLUDEDIR= / -DBoost_INCLUDE_DIR= as a fast path.
foreach(_hint Boost_INCLUDE_DIR BOOST_INCLUDEDIR BOOST_ROOT)
    if(DEFINED ${_hint})
        set(_candidate "${${_hint}}")
        if(EXISTS "${_candidate}/boost/version.hpp")
            set(BOOST_INCLUDE_DIR "${_candidate}" CACHE PATH "Boost include directory" FORCE)
            message(STATUS "[Boost] using hinted path ${BOOST_INCLUDE_DIR}")
            return()
        elseif(EXISTS "${_candidate}/include/boost/version.hpp")
            set(BOOST_INCLUDE_DIR "${_candidate}/include" CACHE PATH "Boost include directory" FORCE)
            message(STATUS "[Boost] using hinted path ${BOOST_INCLUDE_DIR}")
            return()
        endif()
    endif()
endforeach()

# ---------------------------------------------------------------------------
# Stage 2: local tarball cache
# ---------------------------------------------------------------------------
string(REPLACE "." "_" _boost_us "${BOOST_VERSION}")
set(_boost_dirname "boost_${_boost_us}")
set(_boost_tarname_gz "${_boost_dirname}.tar.gz")
set(_boost_tarname_zip "${_boost_dirname}.zip")

set(_boost_tarball "")
foreach(_name IN ITEMS ${_boost_tarname_gz} ${_boost_tarname_zip})
    if(EXISTS "${IOTDB_OS_DEPS_DIR}/${_name}")
        set(_boost_tarball "${IOTDB_OS_DEPS_DIR}/${_name}")
        break()
    endif()
endforeach()

# ---------------------------------------------------------------------------
# Stage 3: download
# ---------------------------------------------------------------------------
if(NOT _boost_tarball)
    if(IOTDB_OFFLINE)
        message(FATAL_ERROR
                "[Boost] IOTDB_OFFLINE=ON but no Boost tarball found in "
                "${IOTDB_OS_DEPS_DIR}. Expected one of: ${_boost_tarname_gz}, ${_boost_tarname_zip}.")
    endif()

    set(_boost_url "https://archives.boost.io/release/${BOOST_VERSION}/source/${_boost_tarname_gz}")
    set(_boost_tarball "${IOTDB_OS_DEPS_DIR}/${_boost_tarname_gz}")
    message(STATUS "[Boost] downloading ${_boost_url}")
    file(DOWNLOAD "${_boost_url}" "${_boost_tarball}"
            SHOW_PROGRESS
            STATUS _boost_dl_status
            TLS_VERIFY ON)
    list(GET _boost_dl_status 0 _boost_dl_code)
    if(NOT _boost_dl_code EQUAL 0)
        list(GET _boost_dl_status 1 _boost_dl_msg)
        file(REMOVE "${_boost_tarball}")
        message(FATAL_ERROR "[Boost] download failed: ${_boost_dl_msg}")
    endif()
endif()

# ---------------------------------------------------------------------------
# Extract headers-only into ${CMAKE_BINARY_DIR}/_deps/boost
# ---------------------------------------------------------------------------
set(_boost_extract_dir "${CMAKE_BINARY_DIR}/_deps/boost")
set(_boost_marker "${_boost_extract_dir}/.extracted-${BOOST_VERSION}")

if(NOT EXISTS "${_boost_marker}")
    file(REMOVE_RECURSE "${_boost_extract_dir}")
    file(MAKE_DIRECTORY "${_boost_extract_dir}")
    message(STATUS "[Boost] extracting ${_boost_tarball}")
    file(ARCHIVE_EXTRACT
            INPUT "${_boost_tarball}"
            DESTINATION "${_boost_extract_dir}")
    file(TOUCH "${_boost_marker}")
endif()

set(BOOST_INCLUDE_DIR "${_boost_extract_dir}/${_boost_dirname}"
        CACHE PATH "Boost include directory" FORCE)

if(NOT EXISTS "${BOOST_INCLUDE_DIR}/boost/version.hpp")
    message(FATAL_ERROR
            "[Boost] Could not locate boost/version.hpp after extraction. "
            "Looked in: ${BOOST_INCLUDE_DIR}")
endif()

message(STATUS "[Boost] BOOST_INCLUDE_DIR = ${BOOST_INCLUDE_DIR}")
