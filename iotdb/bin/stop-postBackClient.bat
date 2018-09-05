@echo off

wmic process where (commandline like "%%postBackClient%%" and not name="wmic.exe") delete
rem ps ax | grep -i 'postBackClient' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM