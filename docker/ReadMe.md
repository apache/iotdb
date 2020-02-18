# How to build

docker build -t THE_DOCKER_IMAGE_NAME:THE_VERSION -f THE_DOCKER_FILE_NAME

e.g.,

```
docker build -t my-iotdb:0.9.1 -f Dockerfile-0.9.1
```

# How to run IoTDB server 

Actually, we maintain a repo on dockerhub, so that you can get the docker image directly.

For example,
```
docker run -d -p 6667:6667 -p 31999:31999 -p 8181:8181 -p 5555:5555 apache/iotdb:0.9.1
```

# How to run IoTDB client

Suppose you have run an IoTDB Server in docker

1. Use `docker ps` to find out the CONTAINER ID
e.g.,
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                                                                NAMES
c82321c70137        apache/iotdb:0.9.1  "/iotdb/sbin/start-s…"   12 minutes ago      Up 12 minutes       0.0.0.0:6667->6667/tcp, 0.0.0.0:8181->8181/tcp, 5555/tcp, 0.0.0.0:31999->31999/tcp   elegant_germain
```
2. Use `docker exec` to attach the container:
```
docker exec -it c82321c70137 /bin/bash
```

Then, run `start-client.sh`

Enjoy it!