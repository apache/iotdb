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

# Description

This module is for using Docker and TestContainer for end to end test.

## Requirements

You have to install Docker before you runn this module.
See [Docker Version Requirement](https://www.testcontainers.org/supported_docker_environment/).

IoTDB project will detect whether a Docker is installed (but do not check the docker's version).

The logic is, for Unix-like system, it checks whether `/var/run/docker.sock` exists.
For Window system, it checks whether `C:\Program Files\Docker\Docker\resources\bin\docker.exe` exists.

If you are sure you have installed the Docker but `testcontainer` module is not loaed, use `-P unixDockerCheck`
in your maven command, which also works on Windows OS.

## Behavior

Before running `integration-test` in this module, binaries must be generated in the `distribution` module,
e.g, call `mvn package -Dmaven.test.skip=true`.

In this module, when running `mvn pre-integration-test` (or `mvn integration-test`, `mvn post-integration-test`),
the module will build docker image, `apache/iotdb:maven-development`.

In the `post-integration-test` phase, the above images will be removed.

In the `integration-test` phase, all `src/test/java/**/*IT.java` will be tested.

## How it runs

`apache/iotdb:maven-development` is generated following the Dockerfile `${basedir}/docker/src/main/Dockerfile-single`, and

For testing sync module only, we use `mvn integration-test -P sync`.

TestContainer can start the docker (or docker compose) automatically.

But these docker compose files can also be used independently.
e.g., `docker-compose up`.


