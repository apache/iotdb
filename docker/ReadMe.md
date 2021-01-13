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

# How to build

docker build -t THE_DOCKER_IMAGE_NAME:THE_VERSION -f THE_DOCKER_FILE_NAME

e.g.,

```
docker build -t my-iotdb:<version> -f Dockerfile-<version>
```

# How to run IoTDB server 

Actually, we maintain a repo on dockerhub, so that you can get the docker image directly.

For example,
```
docker run -d -p 6667:6667 -p 31999:31999 -p 8181:8181 -p 5555:5555 apache/iotdb:<version>
```

## How to configure docker volumes

The instructions below show how to store the output data and logs of IoTDB to two folders called 
iotdb_data and iotdb_logs respectively. 

`/D/docker/iotdb_data` and `/D/docker/iotdb_logs` can be changed to any local directory of your own host.
```
docker run -it -v /D/docker/iotdb_data:/iotdb/data -v /D/docker/iotdb_logs:/iotdb/logs --name 123 apache/iotdb:<version>
```

# How to run IoTDB client

Suppose you have run an IoTDB Server in docker

1. Use `docker ps` to find out the CONTAINER ID
e.g.,
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                                                                NAMES
c82321c70137        apache/iotdb:<version>  "/iotdb/sbin/start-s…"   12 minutes ago      Up 12 minutes       0.0.0.0:6667->6667/tcp, 0.0.0.0:8181->8181/tcp, 5555/tcp, 0.0.0.0:31999->31999/tcp   elegant_germain
```
2. Use `docker exec` to attach the container:
```
docker exec -it c82321c70137 /bin/bash
```

Then, for the latest version (or, >=0.10.x), run `start-cli.sh`, for version 0.9.x and 0.8.1, run `start-client.sh`.

Enjoy it!
