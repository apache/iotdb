#!/bin/sh

PIDS=$(ps ax | grep -i 'tsfiledb\.TsFileDB' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No TsFileDB server to stop"
  exit 1
else 
  kill -s TERM $PIDS
fi
