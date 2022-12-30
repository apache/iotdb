#!/bin/sh
#
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

thrift_bin=/usr/local/bin/thrift
thrift_lib=/usr/local/lib

### if you are compiling for 32bit CPU such as armv7l etc(maybe in an armv7l
### server or using a cross-compile toolchain.
### you should adefine ARCH32 to specify it.
#
# CXX_FLAGS_DEFINED="-DARCH32"

CXX_FLAGS="-O2 -g -std=c++11 -lthrift"
CXX_SO_FLAGS="-shared -fPIC"

source_file="Session.cpp gen-cpp/common_types.cpp gen-cpp/IClientRPCService.cpp gen-cpp/client_types.cpp gen-cpp/IClientRPCService_server.skeleton.cpp"
include_file="Session.h gen-cpp/common_types.h gen-cpp/IClientRPCService.h gen-cpp/client_types.h"

common_thrift_dir=../thrift-commons/src/main/thrift
client_thrift_dir=../thrift/src/main/thrift

$thrift_bin --gen cpp -o src/main/ $common_thrift_dir/common.thrift
$thrift_bin --gen cpp -o src/main/ -I $common_thrift_dir $client_thrift_dir/client.thrift

cd src/main
LD_LIBRARY_PATH=$thrift_lib g++ -Igen-cpp $CXX_FLAGS $CXX_SO_FLAGS $CXX_FLAGS_DEFINED -o libiotdb_session.so $source_file

# back to iotdb/client-cpp
cd ../../
mkdir iotdb_cpp_client_sdk
mkdir iotdb_cpp_client_sdk/include iotdb_cpp_client_sdk/lib
cp $thrift_lib/libthrift-0.14.0.so  iotdb_cpp_client_sdk/lib/
cp src/main/libiotdb_session.so iotdb_cpp_client_sdk/lib/

cp src/main/Session.h iotdb_cpp_client_sdk/include/
cp src/main/gen-cpp/*.h iotdb_cpp_client_sdk/include/

tar cvf iotdb_cpp_client_sdk.tar iotdb_cpp_client_sdk


