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
# GenerateThriftSources.cmake
#
# Generates C++ stubs from the iotdb-protocol Thrift files using the thrift
# compiler that FetchThrift just built.
#
# Inputs (resolved relative to the workspace root):
#   iotdb-protocol/thrift-commons/src/main/thrift/common.thrift
#   iotdb-protocol/thrift-datanode/src/main/thrift/client.thrift
#
# Outputs:
#   ${THRIFT_GEN_CPP_DIR}/*.{h,cpp}     - generated C++ sources
#   THRIFT_GENERATED_SRCS               - list of *.cpp files for linking
#   THRIFT_GENERATED_HDRS               - list of *.h files (informational)
# =============================================================================

if(NOT THRIFT_EXECUTABLE)
    message(FATAL_ERROR "[GenThrift] THRIFT_EXECUTABLE not set - did FetchThrift run first?")
endif()

# Anchor the source-of-truth .thrift files via the workspace root. The CMake
# project lives at <repo>/iotdb-client/client-cpp/, so the workspace root is
# two levels up.
get_filename_component(_workspace_root "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)

set(_common_thrift "${_workspace_root}/iotdb-protocol/thrift-commons/src/main/thrift/common.thrift")
set(_client_thrift "${_workspace_root}/iotdb-protocol/thrift-datanode/src/main/thrift/client.thrift")

foreach(_f IN ITEMS "${_common_thrift}" "${_client_thrift}")
    if(NOT EXISTS "${_f}")
        message(FATAL_ERROR "[GenThrift] missing thrift input: ${_f}")
    endif()
endforeach()

# Mirror the bash reference script: stage both .thrift files into a flat
# working directory so `include "common.thrift"` resolves without -I args.
set(_thrift_proto_dir "${CMAKE_BINARY_DIR}/thrift-protocols")
file(MAKE_DIRECTORY "${_thrift_proto_dir}")
configure_file("${_common_thrift}" "${_thrift_proto_dir}/common.thrift" COPYONLY)
configure_file("${_client_thrift}" "${_thrift_proto_dir}/client.thrift" COPYONLY)

set(THRIFT_GEN_CPP_DIR "${CMAKE_BINARY_DIR}/gen-cpp" CACHE PATH "" FORCE)
file(MAKE_DIRECTORY "${THRIFT_GEN_CPP_DIR}")

# A stamp file lets us re-run thrift only when one of the inputs changes
# (handled by add_custom_command at build time) while also making sure we
# run once at configure time so the initial file(GLOB) finds something.
set(_thrift_stamp "${THRIFT_GEN_CPP_DIR}/.generated-stamp")

set(_thrift_inputs
        "${_thrift_proto_dir}/common.thrift"
        "${_thrift_proto_dir}/client.thrift")

set(_run_thrift FALSE)
if(NOT EXISTS "${_thrift_stamp}")
    set(_run_thrift TRUE)
else()
    foreach(_in IN LISTS _thrift_inputs)
        if("${_in}" IS_NEWER_THAN "${_thrift_stamp}")
            set(_run_thrift TRUE)
            break()
        endif()
    endforeach()
endif()

if(_run_thrift)
    message(STATUS "[GenThrift] running ${THRIFT_EXECUTABLE} on common.thrift / client.thrift")
    foreach(_in IN LISTS _thrift_inputs)
        execute_process(
                COMMAND "${THRIFT_EXECUTABLE}" -r --gen cpp:no_skeleton -out "${THRIFT_GEN_CPP_DIR}" "${_in}"
                WORKING_DIRECTORY "${_thrift_proto_dir}"
                RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "[GenThrift] thrift compile failed for ${_in} (rc=${_rc})")
        endif()
    endforeach()
    # Defensive: remove any accidentally produced server skeleton.
    file(GLOB _skeletons "${THRIFT_GEN_CPP_DIR}/*_server.skeleton.cpp")
    if(_skeletons)
        file(REMOVE ${_skeletons})
    endif()
    file(TOUCH "${_thrift_stamp}")
endif()

# Build-time regeneration: whenever the workspace .thrift files change, rerun
# the thrift compiler. The OUTPUT is the stamp; downstream targets that
# depend on iotdb_thrift_codegen will get the rebuild for free.
add_custom_command(
        OUTPUT "${_thrift_stamp}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different "${_common_thrift}" "${_thrift_proto_dir}/common.thrift"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different "${_client_thrift}" "${_thrift_proto_dir}/client.thrift"
        COMMAND "${THRIFT_EXECUTABLE}" -r --gen cpp:no_skeleton -out "${THRIFT_GEN_CPP_DIR}" "${_thrift_proto_dir}/common.thrift"
        COMMAND "${THRIFT_EXECUTABLE}" -r --gen cpp:no_skeleton -out "${THRIFT_GEN_CPP_DIR}" "${_thrift_proto_dir}/client.thrift"
        COMMAND ${CMAKE_COMMAND} -E touch "${_thrift_stamp}"
        DEPENDS "${_common_thrift}" "${_client_thrift}" iotdb_thrift_external
        WORKING_DIRECTORY "${_thrift_proto_dir}"
        COMMENT "Regenerating thrift C++ stubs"
        VERBATIM)

add_custom_target(iotdb_thrift_codegen DEPENDS "${_thrift_stamp}")

# Glob results with CONFIGURE_DEPENDS so re-cmake picks up newly produced files.
file(GLOB THRIFT_GENERATED_SRCS CONFIGURE_DEPENDS "${THRIFT_GEN_CPP_DIR}/*.cpp")
file(GLOB THRIFT_GENERATED_HDRS CONFIGURE_DEPENDS "${THRIFT_GEN_CPP_DIR}/*.h")

list(FILTER THRIFT_GENERATED_SRCS EXCLUDE REGEX ".*_server\\.skeleton\\.cpp$")

list(LENGTH THRIFT_GENERATED_SRCS _gen_count)
message(STATUS "[GenThrift] generated ${_gen_count} cpp files in ${THRIFT_GEN_CPP_DIR}")
