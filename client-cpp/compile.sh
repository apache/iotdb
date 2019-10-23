#!/bin/bash
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

export SOURCE_DIR=src/
export cpp_program_file_name=src/SessionExample.cpp
export executable_file_name=Example


if [ ! -n "$1" ] ;then
    echo "Default output to "${executable_file_name}
else
    executable_file_name=$1
    echo "Output to "${executable_file_name}
fi

if [ ! -n "$2" ] ;then
    echo "Use the default client "${cpp_program_file_name}
else
    cpp_program_file_name=$2
    echo "Use the client "${cpp_program_file_name}
fi

thrift -gen cpp -out ${SOURCE_DIR}  $(dirname $0)/../service-rpc/src/main/thrift/rpc.thrift
rm ${SOURCE_DIR}TSIService_server.skeleton.cpp
g++ -o ${executable_file_name} ${cpp_program_file_name}  ${SOURCE_DIR}IOTDBSession.cpp ${SOURCE_DIR}rpc_constants.cpp ${SOURCE_DIR}rpc_types.cpp ${SOURCE_DIR}TSIService.cpp -lthrift 
