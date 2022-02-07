#!/bin/bash
echo 3 | sudo tee /proc/sys/vm/drop_caches
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
a=5
for((i=0;i<a;i++)) do
    ./start-server.sh &
    sleep 3s
    java -jar /data3/ruilei/rl/BallSpeed_testspace/QueryBallSpeed-0.12.4.jar $1 $2 $3
    ./stop-server.sh
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    sleep 3s
done
