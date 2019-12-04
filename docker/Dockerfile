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

FROM openjdk:11
RUN apt-get update \
  && apt-get install wget unzip maven -y \
  && wget https://github.com/apache/incubator-iotdb/archive/master.zip \
  && unzip master.zip \
  && rm master.zip
WORKDIR incubator-iotdb-master
RUN mvn package -Papache-release -DskipTests
RUN mkdir /iotdb \
  && mv /incubator-iotdb-master/distribution/target/apache-iotdb-0.10.0-SNAPSHOT-incubating-bin/apache-iotdb-0.10.0-SNAPSHOT-incubating/* /iotdb/
WORKDIR /
RUN ls -lh ~/.m2 \
  && rm -rf ~/.m2 \
  && rm -rf /incubator-iotdb-master \
  && sed -i '119d' /iotdb/conf/logback.xml \
  && apt-get remove wget maven unzip -y \
  && apt-get autoremove -y \
  && apt-get purge --auto-remove -y \
  && apt-get clean -y
EXPOSE 31999
EXPOSE 6667
VOLUME /iotdb/data
VOLUME /iotdb/logs
#ENTRYPOINT ["/iotdb/bin/start-server.sh"]
