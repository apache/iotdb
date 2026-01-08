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

# ===================== Configuration ========================
host=127.0.0.1
rpcPort=6667
user=root
pass=root

# ===================== Resolve JAR Path ========================
jar_abs_path=

# Check if parameter is provided
if [ ! -z "$1" ]; then
  jar_abs_path="$1"
fi

# If no JAR path provided, use default relative path
if [ -z "$jar_abs_path" ]; then
  jar_abs_path="$(realpath ../ext/pipe/library-pipe.jar)"
  echo "[INFO] No jar path provided, using default: $jar_abs_path"
fi

# ===================== Convert to URI ========================
if [[ "$jar_abs_path" =~ ^https:// ]]; then
  uri_jar_path="$jar_abs_path"
else
  uri_jar_path="file:$jar_abs_path"
fi

echo "[INFO] Using jar URI: $uri_jar_path"

# ===================== Register Pipe Plugin ========================
../sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create pipeplugin if not exists MQTTExtractor as 'org.apache.iotdb.libpipe.extractor.mqtt.MQTTExtractor' USING URI '$uri_jar_path'"


