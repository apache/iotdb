#!/bin/sh

if [ -z "${TSFILE_HOME}" ]; then
  export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

"$TSFILE_HOME"/bin/tsfiledb-deamon.sh stop