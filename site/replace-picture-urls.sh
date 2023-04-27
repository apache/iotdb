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
find ./target/vue-source/src -name "*.md" -path "**/Master/**" -print0 | xargs -0 sed -i 's/github.com\/apache\/iotdb-bin-resources\/blob\/main\//gitbox.apache.org\/repos\/asf?p=iotdb-bin-resources.git;a=blob_plain;f=/g'
find ./target/vue-source/src -name "*.md" -path "**/Master/**" -print0 | xargs -0 sed -i 's/?raw=true//g'
find ./target/vue-source/src -name "*.md" -path "**/V1*/**" -print0 | xargs -0 sed -i 's/github.com\/apache\/iotdb-bin-resources\/blob\/main\//gitbox.apache.org\/repos\/asf?p=iotdb-bin-resources.git;a=blob_plain;f=/g'
find ./target/vue-source/src -name "*.md" -path "**/V1*/**" -print0 | xargs -0 sed -i 's/?raw=true//g'
find ./target/vue-source/src -name "*.md" -print0 | xargs -0 sed -i 's/https:\/\/raw.githubusercontent.com\/apache\/iotdb\/master\/site\/src\/main\/.vuepress\/public//g'