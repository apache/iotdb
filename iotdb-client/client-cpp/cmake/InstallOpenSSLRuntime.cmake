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
# InstallOpenSSLRuntime.cmake  (only used when WITH_SSL=ON)
#
# Bundles the OpenSSL shared libraries that iotdb_session links against into the
# package lib/ directory, so the published SDK is self-contained and runs on
# machines that do not have OpenSSL installed.
#
# Relies on a prior find_package(OpenSSL) having populated
# OPENSSL_SSL_LIBRARY / OPENSSL_CRYPTO_LIBRARY / OPENSSL_ROOT_DIR /
# OPENSSL_VERSION_MAJOR.
#
# When OpenSSL was linked statically (the from-source fallback uses no-shared),
# there is nothing to bundle: those objects are already inside libiotdb_session.
# =============================================================================

# Windows: find_package resolves the import .lib; the runtime DLLs live in
# <root>/bin. Collect them, filtering by major version so installs that ship
# several ABIs side by side (e.g. libssl-1_1-x64.dll + libssl-3-x64.dll) only
# bundle the one we actually linked.
function(_iotdb_collect_openssl_windows_dlls _out_var)
    set(_roots "")
    if(OPENSSL_ROOT_DIR)
        list(APPEND _roots "${OPENSSL_ROOT_DIR}")
    endif()
    foreach(_implib IN LISTS OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY OPENSSL_LIBRARIES)
        if(_implib AND EXISTS "${_implib}")
            # Walk up from the import lib (.../lib, .../lib/VC/x64/MD, ...) to find
            # a directory that owns a bin/ holding the DLLs.
            get_filename_component(_dir "${_implib}" DIRECTORY)
            list(APPEND _roots "${_dir}")
            foreach(_up RANGE 1 4)
                get_filename_component(_dir "${_dir}" DIRECTORY)
                list(APPEND _roots "${_dir}")
            endforeach()
        endif()
    endforeach()
    list(REMOVE_DUPLICATES _roots)

    set(_dlls "")
    set(_seen_names "")
    foreach(_root IN LISTS _roots)
        if(_root AND IS_DIRECTORY "${_root}")
            file(GLOB _found
                    "${_root}/bin/libssl-${OPENSSL_VERSION_MAJOR}*.dll"
                    "${_root}/bin/libcrypto-${OPENSSL_VERSION_MAJOR}*.dll"
                    "${_root}/libssl-${OPENSSL_VERSION_MAJOR}*.dll"
                    "${_root}/libcrypto-${OPENSSL_VERSION_MAJOR}*.dll")
            # The same DLL can appear under several candidate roots (e.g. bin/ and
            # the install root); keep only the first occurrence of each filename.
            foreach(_dll IN LISTS _found)
                get_filename_component(_name "${_dll}" NAME)
                if(NOT _name IN_LIST _seen_names)
                    list(APPEND _seen_names "${_name}")
                    list(APPEND _dlls "${_dll}")
                endif()
            endforeach()
        endif()
    endforeach()
    set(${_out_var} "${_dlls}" PARENT_SCOPE)
endfunction()

function(iotdb_install_openssl_runtime)
    if(WIN32)
        _iotdb_collect_openssl_windows_dlls(_dlls)
        if(NOT _dlls)
            message(STATUS
                    "[OpenSSL] no runtime DLLs found to bundle; ensure the OpenSSL "
                    "bin/ directory is on PATH when running the SDK")
            return()
        endif()
        foreach(_dll IN LISTS _dlls)
            message(STATUS "[OpenSSL] bundling runtime library into lib/: ${_dll}")
        endforeach()
        install(FILES ${_dlls} DESTINATION lib)
        return()
    endif()

    # Linux / macOS: OPENSSL_*_LIBRARY is the developer name (libssl.so /
    # libssl.dylib), usually a symlink to the SONAME (libssl.so.3 / .1.1).
    # FOLLOW_SYMLINK_CHAIN installs the whole chain with the symlinks preserved,
    # so the loader finds the SONAME the binary records. Static archives (.a)
    # are skipped: they are already linked into libiotdb_session.
    set(_files_arg "")
    set(_have_libs OFF)
    foreach(_lib IN LISTS OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY)
        if(_lib AND EXISTS "${_lib}" AND NOT _lib MATCHES "\\.a$")
            string(APPEND _files_arg " \"${_lib}\"")
            set(_have_libs ON)
            message(STATUS "[OpenSSL] bundling runtime library into lib/: ${_lib}")
        endif()
    endforeach()

    if(NOT _have_libs)
        message(STATUS
                "[OpenSSL] no shared runtime libraries to bundle "
                "(OpenSSL linked statically); SDK is self-contained")
        return()
    endif()

    install(CODE
            "file(INSTALL DESTINATION \"\${CMAKE_INSTALL_PREFIX}/lib\"
                  TYPE SHARED_LIBRARY FOLLOW_SYMLINK_CHAIN
                  FILES ${_files_arg})")
endfunction()
