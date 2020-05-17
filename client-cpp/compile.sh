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

export THRIFT_EXE=thrift
export CMAKE_EXE=cmake
export MAKE_EXE=make
export SH_DIR=$(dirname $0)/
export THRIFT_SCRIPT=${SH_DIR}rpc.thrift
export THRIFT_OUT=${SH_DIR}gen-cpp
export USELESS_FILE=${SH_DIR}/gen-cpp/TSIService_server.skeleton.cpp
export SUB_CMAKELISTS=${SH_DIR}/gen-cpp/CMakeLists.txt
export BUILD_FOLDER=${SH_DIR}/build

rm -rf ${THRIFT_OUT}
${THRIFT_EXE} -gen cpp ${THRIFT_SCRIPT}
rm -rf ${USELESS_FILE}
touch ${SUB_CMAKELISTS}
cd ${BUILD_FOLDER}
${CMAKE_EXE} ../
${MAKE_EXE}
mv client-cpp ../