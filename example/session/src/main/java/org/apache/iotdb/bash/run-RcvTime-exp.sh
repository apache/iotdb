#!/bin/bash

############################
# extract small data for fast exp
# java ExtractFullGameData inPath outPath timeIdx valueIdx lineNum
# lineNum=-1 means extracting total lines
############################
cd /data3/raw_data/rl/RcvTime

############################
# generate out-of-order source data
# java OverlapGenerator inPath outPath timeIdx valueIdx overlapPercentage overlapDepth
# overlapPercentage [0,100]
# overlapDepth [0,50]
############################
cp RcvTime.csv RcvTime-O_0_0

java OverlapGenerator RcvTime.csv RcvTime-O_10_10 0 1 10 10
java OverlapGenerator RcvTime.csv RcvTime-O_30_10 0 1 30 10
java OverlapGenerator RcvTime.csv RcvTime-O_50_10 0 1 50 10
java OverlapGenerator RcvTime.csv RcvTime-O_70_10 0 1 70 10
java OverlapGenerator RcvTime.csv RcvTime-O_90_10 0 1 90 10

############################
# /data3/raw_data/rl
# ---- RcvTime
# /data3/ruilei/rl/dataSpace
# /data3/ruilei/rl/iotdb-server-0.12.4
# /data3/ruilei/rl/iotdb-engine-example.properties
# /data3/ruilei/rl/tool.sh
# /data3/ruilei/rl/ProcessResult.class
# /data3/ruilei/rl/ProcessResult.java
# /data3/ruilei/rl/SumResultUnify.class
# /data3/ruilei/rl/SumResultUnify.java
# /data3/ruilei/rl/RcvTime_testspace
# ---- WriteRcvTime-0.12.4.jar [make sure updated][time ms ms value long float type][device measurement path]
# ---- QueryFullGame-0.12.4.jar [make sure updated][time ms ms value long float type][device measurement path]
# ---- query_experiment.sh [make sure updated][call jar name]
# [EXP1 EXP2]
# ---- O_10_10_D_0_0:
# ---- ---- iotdb-engine-enableCPVtrue.properties
# ---- ---- iotdb-engine-enableCPVfalse.properties
# ---- ---- write_data.sh
# ---- ---- vary_w
# ---- ---- ---- moc
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- mac
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- cpv
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- vary_tqe
# ---- ---- ---- moc
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- mac
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- cpv
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# [EXP3]
# ---- O_0_0_D_0_0:
# ---- ---- iotdb-engine-enableCPVtrue.properties
# ---- ---- iotdb-engine-enableCPVfalse.properties
# ---- ---- write_data.sh
# ---- ---- fix
# ---- ---- ---- moc
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- mac
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- cpv
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- O_30_10_D_0_0
# ---- O_50_10_D_0_0
# ---- O_70_10_D_0_0
# ---- O_90_10_D_0_0
# [EXP4]
# ---- O_10_10_D_9_10
# ---- O_10_10_D_29_10
# ---- O_10_10_D_49_10
# ---- O_10_10_D_69_10
# ---- O_10_10_D_89_10
# [EXP5]
# ---- O_10_10_D_9_30
# ---- O_10_10_D_9_50
# ---- O_10_10_D_9_70
# ---- O_10_10_D_9_90

############################

############################
# [EXP1]
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_0_0
cd O_10_10_D_0_0

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_0_0/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_0_0/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_0_0/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 0 0 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_0_0
mkdir vary_w
cd vary_w

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 1 2 >> result_1.txt
java ProcessResult result_1.txt result_1.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 2 2 >> result_2.txt
java ProcessResult result_2.txt result_2.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 10 2 >> result_4.txt
java ProcessResult result_4.txt result_4.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 20 2 >> result_5.txt
java ProcessResult result_5.txt result_5.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 50 2 >> result_6.txt
java ProcessResult result_6.txt result_6.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 100 2 >> result_7.txt
java ProcessResult result_7.txt result_7.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 200 2 >> result_8.txt
java ProcessResult result_8.txt result_8.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 500 2 >> result_9.txt
java ProcessResult result_9.txt result_9.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 1000 2 >> result_10.txt
java ProcessResult result_10.txt result_10.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 1 1 >> result_1.txt
java ProcessResult result_1.txt result_1.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 2 1 >> result_2.txt
java ProcessResult result_2.txt result_2.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 10 1 >> result_4.txt
java ProcessResult result_4.txt result_4.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 20 1 >> result_5.txt
java ProcessResult result_5.txt result_5.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 50 1 >> result_6.txt
java ProcessResult result_6.txt result_6.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 100 1 >> result_7.txt
java ProcessResult result_7.txt result_7.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 200 1 >> result_8.txt
java ProcessResult result_8.txt result_8.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 500 1 >> result_9.txt
java ProcessResult result_9.txt result_9.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 1000 1 >> result_10.txt
java ProcessResult result_10.txt result_10.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 1 3 >> result_1.txt
java ProcessResult result_1.txt result_1.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 2 3 >> result_2.txt
java ProcessResult result_2.txt result_2.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 10 3 >> result_4.txt
java ProcessResult result_4.txt result_4.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 20 3 >> result_5.txt
java ProcessResult result_5.txt result_5.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 50 3 >> result_6.txt
java ProcessResult result_6.txt result_6.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 100 3 >> result_7.txt
java ProcessResult result_7.txt result_7.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 200 3 >> result_8.txt
java ProcessResult result_8.txt result_8.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 500 3 >> result_9.txt
java ProcessResult result_9.txt result_9.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 1000 3 >> result_10.txt
java ProcessResult result_10.txt result_10.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# [EXP2]
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_0_0
mkdir vary_tqe
cd vary_tqe

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 509033300 5 2 >> result_1.txt
java ProcessResult result_1.txt result_1.out ../sumResultMOC.csv
./../../../query_experiment.sh 1018066600 5 2 >> result_2.txt
java ProcessResult result_2.txt result_2.out ../sumResultMOC.csv
./../../../query_experiment.sh 2036133200 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv
./../../../query_experiment.sh 5090333000 5 2 >> result_4.txt
java ProcessResult result_4.txt result_4.out ../sumResultMOC.csv
./../../../query_experiment.sh 10180666000 5 2 >> result_5.txt
java ProcessResult result_5.txt result_5.out ../sumResultMOC.csv
./../../../query_experiment.sh 26461736001 5 2 >> result_6.txt
java ProcessResult result_6.txt result_6.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 509033300 5 1 >> result_1.txt
java ProcessResult result_1.txt result_1.out ../sumResultMAC.csv
./../../../query_experiment.sh 1018066600 5 1 >> result_2.txt
java ProcessResult result_2.txt result_2.out ../sumResultMAC.csv
./../../../query_experiment.sh 2036133200 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv
./../../../query_experiment.sh 5090333000 5 1 >> result_4.txt
java ProcessResult result_4.txt result_4.out ../sumResultMAC.csv
./../../../query_experiment.sh 10180666000 5 1 >> result_5.txt
java ProcessResult result_5.txt result_5.out ../sumResultMAC.csv
./../../../query_experiment.sh 26461736001 5 1 >> result_6.txt
java ProcessResult result_6.txt result_6.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 509033300 5 3 >> result_1.txt
java ProcessResult result_1.txt result_1.out ../sumResultCPV.csv
./../../../query_experiment.sh 1018066600 5 3 >> result_2.txt
java ProcessResult result_2.txt result_2.out ../sumResultCPV.csv
./../../../query_experiment.sh 2036133200 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv
./../../../query_experiment.sh 5090333000 5 3 >> result_4.txt
java ProcessResult result_4.txt result_4.out ../sumResultCPV.csv
./../../../query_experiment.sh 10180666000 5 3 >> result_5.txt
java ProcessResult result_5.txt result_5.out ../sumResultCPV.csv
./../../../query_experiment.sh 26461736001 5 3 >> result_6.txt
java ProcessResult result_6.txt result_6.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

# [EXP3]
# ---- O_0_0_D_0_0:
# ---- ---- iotdb-engine-enableCPVtrue.properties
# ---- ---- iotdb-engine-enableCPVfalse.properties
# ---- ---- write_data.sh
# ---- ---- fix
# ---- ---- ---- moc
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- mac
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- ---- ---- cpv
# ---- ---- ---- ---- change_interval_experiments.sh
# ---- ---- ---- ---- ProcessResult.class
# ---- ---- ---- ---- ProcessResult.java
# ---- O_30_10_D_0_0
# ---- O_50_10_D_0_0
# ---- O_70_10_D_0_0
# ---- O_90_10_D_0_0

############################
# O_0_0_D_0_0
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_0_0_D_0_0
cd O_0_0_D_0_0

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_0_0_D_0_0/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_0_0_D_0_0/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_0_0_D_0_0/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_0_0 0 0 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_0_0_D_0_0
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_30_10_D_0_0
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_30_10_D_0_0
cd O_30_10_D_0_0

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_30_10_D_0_0/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_30_10_D_0_0/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_30_10_D_0_0/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_30_10 0 0 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_30_10_D_0_0
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_50_10_D_0_0
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_50_10_D_0_0
cd O_50_10_D_0_0

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_50_10_D_0_0/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_50_10_D_0_0/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_50_10_D_0_0/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_50_10 0 0 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_50_10_D_0_0
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_70_10_D_0_0
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_70_10_D_0_0
cd O_70_10_D_0_0

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_70_10_D_0_0/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_70_10_D_0_0/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_70_10_D_0_0/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_70_10 0 0 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_70_10_D_0_0
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_90_10_D_0_0
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_90_10_D_0_0
cd O_90_10_D_0_0

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_90_10_D_0_0/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_90_10_D_0_0/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_90_10_D_0_0/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_90_10 0 0 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_90_10_D_0_0
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

# [EXP4]
# ---- O_10_10_D_0_0
# ---- O_10_10_D_9_10
# ---- O_10_10_D_29_10
# ---- O_10_10_D_49_10
# ---- O_10_10_D_69_10
# ---- O_10_10_D_89_10

############################
# O_10_10_D_9_10
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_9_10
cd O_10_10_D_9_10

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_9_10/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_9_10/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_9_10/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 9 10 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_9_10
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_10_10_D_29_10
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_29_10
cd O_10_10_D_29_10

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_29_10/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_29_10/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_29_10/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 29 10 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_29_10
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_10_10_D_49_10
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_49_10
cd O_10_10_D_49_10

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_10/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_10/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_10/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 49 10 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_10
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_10_10_D_69_10
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_69_10
cd O_10_10_D_69_10

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_69_10/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_69_10/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_69_10/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 69 10 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_69_10
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_10_10_D_89_10
############################

cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_89_10
cd O_10_10_D_89_10

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_89_10/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_89_10/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_89_10/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 89 10 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_89_10
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

# [EXP5]
# ---- O_10_10_D_49_30
# ---- O_10_10_D_49_50
# ---- O_10_10_D_49_70
# ---- O_10_10_D_49_90

############################
# O_10_10_D_49_30
############################
cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_49_30
cd O_10_10_D_49_30

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_30/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_30/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_30/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 49 30 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_30
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_10_10_D_49_50
############################
cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_49_50
cd O_10_10_D_49_50

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_50/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_50/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_50/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 49 50 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_50
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_10_10_D_49_70
############################
cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_49_70
cd O_10_10_D_49_70

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_70/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_70/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_70/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 49 70 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_70
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

############################
# O_10_10_D_49_90
############################
cd /data3/ruilei/rl/RcvTime_testspace
mkdir O_10_10_D_49_90
cd O_10_10_D_49_90

############################
# prepare iotdb-engine-enableCPVtrue.properties and iotdb-engine-enableCPVfalse.properties
############################
./../../tool.sh enable_CPV true ../../iotdb-engine-example.properties
./../../tool.sh system_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_90/system ../../iotdb-engine-example.properties
./../../tool.sh data_dirs /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_90/data ../../iotdb-engine-example.properties
./../../tool.sh wal_dir /data3/ruilei/rl/dataSpace/RcvTime_O_10_10_D_49_90/wal ../../iotdb-engine-example.properties
./../../tool.sh timestamp_precision ms ../../iotdb-engine-example.properties

./../../tool.sh unseq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh seq_tsfile_size 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh avg_series_point_number_threshold 10000 ../../iotdb-engine-example.properties
./../../tool.sh compaction_strategy NO_COMPACTION ../../iotdb-engine-example.properties
./../../tool.sh enable_unseq_compaction false ../../iotdb-engine-example.properties
./../../tool.sh page_size_in_byte 1073741824 ../../iotdb-engine-example.properties
./../../tool.sh rpc_address 0.0.0.0 ../../iotdb-engine-example.properties
./../../tool.sh rpc_port 6667 ../../iotdb-engine-example.properties

cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVtrue.properties

./../../tool.sh enable_CPV false ../../iotdb-engine-example.properties
cp ../../iotdb-engine-example.properties iotdb-engine-enableCPVfalse.properties

############################
# run write_data.sh
# java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar filePath deleteFreq deleteLen timeIdx valueIdx
############################
cp iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
cd /data3/ruilei/rl/iotdb-server-0.12.4/sbin
./start-server.sh &
sleep 3s
java -jar /data3/ruilei/rl/RcvTime_testspace/WriteRcvTime-0.12.4.jar /data3/raw_data/rl/RcvTime/RcvTime-O_10_10 49 90 0 1
sleep 3s
./stop-server.sh
sleep 3s
echo 3 | sudo tee /proc/sys/vm/drop_caches


############################
# run change_interval_experiments.sh for each approach
# ./../../../query_experiment.sh tqe w approach
############################
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_90
mkdir fix
cd fix

mkdir moc
cd moc
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 2 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMOC.csv

cd ..
mkdir mac
cd mac
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVfalse.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 1 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultMAC.csv

cd ..
mkdir cpv
cd cpv
cp /data3/ruilei/rl/ProcessResult.* .
cp ../../iotdb-engine-enableCPVtrue.properties /data3/ruilei/rl/iotdb-server-0.12.4/conf/iotdb-engine.properties
./../../../query_experiment.sh 26461736001 5 3 >> result_3.txt
java ProcessResult result_3.txt result_3.out ../sumResultCPV.csv

cd ..
cp /data3/ruilei/rl/SumResultUnify.* .
java SumResultUnify sumResultMOC.csv sumResultMAC.csv sumResultCPV.csv result.csv

#########################
# sum results
# [EXP3]
# ---- O_0_0_D_0_0
# ---- O_30_10_D_0_0
# ---- O_50_10_D_0_0
# ---- O_70_10_D_0_0
# ---- O_90_10_D_0_0
# [EXP4]
# ---- O_10_10_D_9_10
# ---- O_10_10_D_29_10
# ---- O_10_10_D_49_10
# ---- O_10_10_D_69_10
# ---- O_10_10_D_89_10
# [EXP5]
# ---- O_10_10_D_49_30
# ---- O_10_10_D_49_50
# ---- O_10_10_D_49_70
# ---- O_10_10_D_49_90
#########################
# [EXP3]
cd /data3/ruilei/rl/RcvTime_testspace/O_0_0_D_0_0
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp3.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_30_10_D_0_0
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp3.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_50_10_D_0_0
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp3.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_70_10_D_0_0
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp3.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_90_10_D_0_0
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp3.csv

# [EXP4]
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_9_10
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp4.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_29_10
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp4.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_10
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp4.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_69_10
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp4.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_89_10
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp4.csv

# [EXP5]
cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_10
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp5.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_30
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp5.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_50
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp5.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_70
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp5.csv

cd /data3/ruilei/rl/RcvTime_testspace/O_10_10_D_49_90
cd fix
cat result.csv >>/data3/ruilei/rl/RcvTime_testspace/exp5.csv
