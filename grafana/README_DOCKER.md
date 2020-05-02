<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Docker Container for IoTDB Grafana Adapter

## Build

* `mvn clean package`
* in the module root execute `docker image build -t apache/iotdb-grafana:{version} .` where version is e.g. 0.9.3.
* Optionally push (if you are logged in) with `docker push jfeinauer/iotdb-grafana:{version}`.

## Execute

Needed Environment Parameters are:

* IOTDB_HOST
* IOTDB_PORT
* IOTDB_USER
* IOTDB_PASSWORD

First, start a container for iotdb:

```
docker run -d -p 6667:6667 -p 31999:31999 -p 8181:8181 --name some-iotdb apache/iotdb:0.9.1-jre8
```

then you can start the adapter via

```
docker run -d --link some-iotdb -e IOTDB_HOST=some-iotdb -e IOTDB_PORT=6667 -e IOTDB_USER=root -e IOTDB_PASSWORD=root --name iotdb-grafana apache/iotdb-grafana:0.9.3
```