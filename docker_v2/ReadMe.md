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

# TimechoDB Docker v2

## Scope

`docker_v2` is the enterprise-only Docker packaging path for TimechoDB. It builds
and runs the enterprise distribution package:

```text
timechodb-${version}-bin.zip
```

The v2 image no longer builds OSS split images. A single enterprise image is
built from `Dockerfile-1.0.0-standalone`, and the runtime role is selected by
the container command.

`docker_v2` is kept separate from the existing `docker/` directory during this
migration. The current v2 scope covers ConfigNode and DataNode containers only.
AINode packaging is intentionally out of scope for v2 at this stage.

Issues fixed in `docker_v2` are not automatically backported to the legacy
`docker/` directory unless explicitly required by a separate change.

## Supported Roles

The image entrypoint is:

```text
/iotdb/sbin/entrypoint.sh
```

Supported commands:

```text
confignode
datanode
```

`confignode` and `datanode` start only the corresponding node process. The
`docker-compose-1c1d.yml` template deploys one ConfigNode container and one
DataNode container with the same enterprise image.

## Build

Put the enterprise distribution zip under `docker_v2/src/main/target`:

```shell
ls docker_v2/src/main/target/timechodb-${version}-bin.zip
```

Build the enterprise Docker image:

```shell
cd docker_v2/src/main/DockerCompose
./do-docker-build-enterprise.sh -t standalone -v ${version}
```

Build and push the `latest` tag:

```shell
./do-docker-build-enterprise.sh -t latest -v ${version} -p
```

The enterprise Docker image version format follows the legacy enterprise Docker
files:

```text
nexus.infra.timecho.com:8243/timecho/timechodb:${version}-standalone
```

## Run With Docker Compose

The compose examples use an external Docker network. Create it once before
running the examples:

```shell
docker network create --subnet=172.18.0.0/16 timechodb
```

One ConfigNode and one DataNode:

```shell
cd docker_v2/src/main/DockerCompose
docker compose -f docker-compose-1c1d.yml up -d
```

One ConfigNode and two DataNodes:

```shell
docker compose -f docker-compose-cluster-1c2d.yml up -d
```

Three ConfigNodes and three DataNodes:

```shell
docker compose -f docker-compose-cluster-3c3d.yml up -d
```

Single ConfigNode container:

```shell
docker compose -f docker-compose-confignode.yml up -d
```

Single DataNode container:

```shell
docker compose -f docker-compose-datanode.yml up -d
```

Each compose service has its own `mem_limit`. Docker writes the value to the
container cgroup, and `entrypoint.sh` uses that cgroup memory limit to size JVM
heap and direct memory:

- single DataNode container: 80% of container memory is assigned to DataNode
- single ConfigNode container: 80% of container memory is assigned to ConfigNode

The compose examples are intended for `docker compose up`. If you adapt them for
Docker Swarm with `docker stack deploy`, do not rely on `mem_limit`; set Swarm
resource limits for each service instead:

```yaml
deploy:
  resources:
    limits:
      memory: 8g
```

Without an effective container memory limit, `entrypoint.sh` falls back to the
host-visible memory when sizing JVM heap and direct memory, which may overcommit
the host and lead to OOM. Swarm deployments may also need other compose changes,
such as network and container-name settings.

CPU detection is left to JDK 17 container support. Disk capacity and free-space
checks are handled by the running TimechoDB Java process.

## Run With Docker

Single DataNode example:

```shell
docker run -d \
  --name timechodb-datanode \
  --memory=8g \
  -p 6667:6667 \
  -e dn_seed_config_node=timechodb-confignode:10710 \
  nexus.infra.timecho.com:8243/timecho/timechodb:${version}-standalone \
  datanode
```

## Client

Attach to a running container and start the CLI:

```shell
docker exec -it timechodb-datanode-1 start-cli.sh
```
