# !/bin/bash
# This file is used to create UDFs of IoTDB-Quality automatically.
# Created by Data Quality Group, School of Software, Tsinghua University
# 

# Parameters
host=127.0.0.1
rpcPort=6667
user=root
pass=root

# Data Quality
./sbin/start-cli.sh -h $host -p $rpcPort -u $user -pw $pass -e "create function validityUDF as 'apache.iotdb.quality.validity.UDTFValidity'"
