#!/bin/sh

PIDS=$(ps ax | grep -i 'IoTDB' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No IoTDB server to stop"
  exit 1
else 
  kill -s TERM $PIDS
  echo "close IoTDB"
fi
