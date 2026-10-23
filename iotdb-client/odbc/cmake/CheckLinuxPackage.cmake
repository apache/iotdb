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
if(NOT EXISTS "${DRIVER}")
    message(FATAL_ERROR "Missing driver: ${DRIVER}")
endif()
if(NOT DEFINED GLIBC_BASELINE)
    message(FATAL_ERROR "GLIBC_BASELINE must come from the shared native Linux release configuration")
endif()
find_program(READELF readelf REQUIRED)
execute_process(COMMAND "${READELF}" --version-info "${DRIVER}"
    OUTPUT_VARIABLE versions RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Cannot inspect ELF versions")
endif()
string(REGEX MATCHALL "GLIBC_[0-9]+\\.[0-9]+(\\.[0-9]+)?" requirements "${versions}")
if(NOT requirements)
    message(FATAL_ERROR "Cannot determine GLIBC requirements")
endif()
foreach(requirement IN LISTS requirements)
    string(REPLACE "GLIBC_" "" version "${requirement}")
    if(version VERSION_GREATER GLIBC_BASELINE)
        message(FATAL_ERROR "${DRIVER} requires ${requirement}, baseline is ${GLIBC_BASELINE}")
    endif()
endforeach()
execute_process(COMMAND "${READELF}" -d "${DRIVER}" OUTPUT_VARIABLE dynamic RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Cannot inspect ELF dependencies")
endif()
string(REGEX MATCHALL "Shared library: \\[[^]]+\\]" dependencies "${dynamic}")
foreach(dependency IN LISTS dependencies)
    string(REGEX REPLACE "Shared library: \\[([^]]+)\\]" "\\1" name "${dependency}")
    if(NOT name MATCHES "^(lib(c|m|dl|pthread|rt)\\.so\\.[0-9]+|libodbcinst\\.so\\.2|ld-linux-x86-64\\.so\\.2)$")
        message(FATAL_ERROR "Unexpected runtime dependency: ${name}")
    endif()
endforeach()
if(dynamic MATCHES "(RPATH|RUNPATH)")
    message(FATAL_ERROR "Release driver must not contain a build-time library search path")
endif()
message(STATUS "Single-library dependency check and GLIBC <= ${GLIBC_BASELINE} passed")
