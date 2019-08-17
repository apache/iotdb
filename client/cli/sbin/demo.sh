# !/bin/bash

host=127.0.0.1
port=6667
user=root
pass=root

./start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "set storage group to root.demo"
./start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "create timeseries root.demo.s1 WITH DATATYPE=INT32, ENCODING=RLE"
./start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(1,10)"
./start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(2,11)"
./start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(3,12)"
./start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "select s1 from root.demo"