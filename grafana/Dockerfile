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
FROM openjdk:11-jre-slim
LABEL maintainer="j.feinauer@pragmaticminds.de"
RUN mkdir /iotdb

ADD target/iotdb-grafana-*.war /iotdb/iotdb-grafana.war
ADD src/main/resources/application.properties /iotdb/

EXPOSE 8888
EXPOSE 4000

WORKDIR /iotdb/
ENTRYPOINT ["java", "-Dspring.datasource.url=jdbc:iotdb://${IOTDB_HOST}:${IOTDB_PORT}", "-Dspring.datasource.username=${IOTDB_USER}", "-Dspring.datasource.password=${IOTDB_PASSWORD}", "-jar", "/iotdb/iotdb-grafana.war"]
