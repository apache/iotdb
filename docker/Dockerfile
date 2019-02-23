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

FROM ubuntu:18.04
RUN apt update \
  && apt install wget unzip lsof openjdk-11-jdk maven -y \
  && wget https://github.com/apache/incubator-iotdb/archive/master.zip \
  && unzip master.zip \
  && rm master.zip \
  && cd incubator-iotdb-master \
  && mvn package -DskipTests \
  && cp -r iotdb/iotdb ~/iotdb \
  && mvn clean \
  && mvn dependency:purge-local-repository -DactTransitively=false \
  && apt remove openjdk-11-jdk wget maven unzip -y \
  && apt install openjdk-11-jre -y \
  && apt autoremove
  && apt purge --auto-remove -y \
  && apt clean -y \
  && java -version \

