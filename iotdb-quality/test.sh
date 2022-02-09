#!/bin/bash
# delete last docker image & container.
docker stop iotdb-quality-cpy
docker rm iotdb-quality-cpy
docker rmi iotdb-quality:cpy
echo Docker image deleted.
# create docker image & container
docker build -t iotdb-quality:cpy -f Dockerfile .
docker run --name iotdb-quality-cpy -d -p 6667:6667 -p 31999:31999 -p 8181:8181 -p 5555:5555 iotdb-quality:cpy /bin/bash
sleep 30s
# test scripts here
echo Test started.
sudo docker exec iotdb-quality-cpy /bin/sh -c "mv /iotdb-quality-0.1.0-jar-with-dependencies.jar /iotdb/ext/udf/"
cd /media/sf_sharedfolder/iotdb-benchmark-dev_quality_ci/bin/
# sh benchmark.sh
sh startup.sh
echo Test finished.
# delete docker image & container.
docker stop iotdb-quality-cpy
docker rm iotdb-quality-cpy
docker rmi iotdb-quality:cpy
echo Docker image deleted.