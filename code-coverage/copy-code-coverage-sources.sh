#!/usr/bin/env bash
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

set -euo pipefail
shopt -s nullglob

cd "$(dirname "$0")"
mkdir -p ./target/classes

for module in \
  ../iotdb-api/* \
  ../iotdb-client/* \
  ../iotdb-core/* \
  ../iotdb-core/metrics/* \
  ../library-udf \
  ../external-service-impl/*
do
  for file in "$module"/target/*.exec
  do
    cp "$file" ./target/
  done

  if [ -d "$module/src/main/java" ] && [ -d "$module/target/classes" ]; then
    echo "copy from ${module}/target/classes"
    cp -r "$module/target/classes/." ./target/classes/
  fi
done
