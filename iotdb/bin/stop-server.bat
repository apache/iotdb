@echo off

wmic process where (commandline like "%%tsfiledb.IoTDB%%" and not name="wmic.exe") delete
rem ps ax | grep -i 'tsfiledb.IoTDB' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM