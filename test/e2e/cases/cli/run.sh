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

set -e

cd "$(dirname "$0")"

docker-compose up -d

max_attempts=10
attempts=1

while ! docker-compose logs | grep -c 'Ready to Run IoTDB E2E Tests' > /dev/null 2>&1; do
  if [[ $attempts -gt $max_attempts ]]; then
    echo "Preparation is not ready after $max_attempts attempts, will exit now"
    exit 1
  fi
  echo "Preparation is not ready yet, retrying ($attempts/$max_attempts)"
  sleep 3
  attempts=$((attempts+1))
done

results=$(docker-compose exec -T server /iotdb/sbin/start-cli.sh -e 'SELECT temperature FROM root.ln.wf01.wt01')

if [[ $results != *"Total line number = 2"* ]]; then
  echo "Total line number should be 2"
  echo "$results"
  exit 1
fi

cd -
