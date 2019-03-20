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
git checkout checkstyle.xml
git checkout java-google-style.xml
git checkout example/**/pom.xml
git checkout grafana/pom.xml
git checkout hadoop/pom.xml
git checkout example/pom.xml
git checkout jdbc/pom.xml
git checkout service-rpc/pom.xml
git checkout spark/pom.xml
git checkout tsfile/pom.xml
git checkout iotdb-cli/pom.xml
git checkout iotdb/pom.xml
git checkout pom.xml
git checkout iotdb-cli/src/test/resources/logback.xml
git checkout iotdb/iotdb/conf/logback.xml
git checkout iotdb/src/test/resources/logback.xml
git checkout tsfile/src/test/resources/logback.xml
