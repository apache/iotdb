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

CXX_FLAGS="-O2 -g -std=c++11 "
IOTDB_SESSION_SDK=../iotdb_cpp_client_sdk

export LD_LIBRARY_PATH=${IOTDB_SESSION_SDK}/lib
g++ $CXX_FLAGS -o demo demo.cpp -liotdb_session -lthrift -L${IOTDB_SESSION_SDK}/lib -I${IOTDB_SESSION_SDK}/include
g++ $CXX_FLAGS -o SessionExample SessionExample.cpp -liotdb_session -lthrift -L${IOTDB_SESSION_SDK}/lib -I${IOTDB_SESSION_SDK}/include
g++ $CXX_FLAGS -o AlignedTimeseriesSessionExample AlignedTimeseriesSessionExample.cpp -liotdb_session -lthrift -L${IOTDB_SESSION_SDK}/lib -I${IOTDB_SESSION_SDK}/include

## run test like this:
# DEMO__total_row_count=20 DEMO__cmd='a' DEMO__max_row_num=5 DEMO__measurement_num=3 ./demo
# ./SessionExample
# ./AlignedTimeseriesSessionExample
