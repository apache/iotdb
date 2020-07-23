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

# this docker file shows how to build an IoTDB image from IoTDB source codes.
FROM ubuntu:18.04
RUN apt update \
  && apt install wget unzip lsof maven thrift-compiler=0.9.1-2.1 -y \
  && wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz -O jdk11.tar.gz \
  && tar -xzf jdk11.tar.gz \
  && rm -rf jdk11.tar.gz \
  && export JAVA_HOME=/jdk-11.0.2/ \
  && export PATH="$JAVA_HOME/bin:$PATH" \
  && wget https://github.com/apache/incubator-iotdb/archive/master.zip \
  && unzip master.zip \
  && rm master.zip \
  && cd incubator-iotdb-master \
  && mvn package -pl server,client -am -Papache-release -DskipTests -Dthrift.download-url="http://www.apache.org/licenses/LICENSE-2.0.txt" -Dthrift.exec.absolute.path="/usr/bin/thrift" \
  && cd target/ \
  && unzip apache-iotdb-0.11.0-SNAPSHOT-incubating-bin.zip \
  && mkdir /iotdb \
  && mv apache-iotdb-0.11.0-SNAPSHOT-incubating/* /iotdb/ \
  && cd ../../ \
  && mvn clean \
  && ls -lh ~/.m2 \
  && rm -rf ~/.m2 \
  && rm -rf /incubator-iotdb-master \
  && sed -i '119d' /iotdb/conf/logback.xml \
  && apt remove wget maven unzip thrift-compiler -y \
  && apt autoremove -y \
  && apt purge --auto-remove -y \
  && apt clean -y 
ENV JAVA_HOME  "/jdk-11.0.2"
ENV PATH "$JAVA_HOME/bin:$PATH"
EXPOSE 6667
VOLUME /iotdb/data
VOLUME /iotdb/logs
#ENTRYPOINT ["/iotdb/bin/start-server.sh"]
