@echo off

wmic process where (commandline like "%%iotdb.IoTDB%%" and not name="wmic.exe") delete
rem ps ax | grep -i 'iotdb.IoTDB' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM
