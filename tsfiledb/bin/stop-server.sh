#!/bin/sh

if [ -z "${TSFILE_HOME}" ]; then
  export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

pid="$TSFILE_HOME"/tsfiledb.pid

kill `cat $pid`