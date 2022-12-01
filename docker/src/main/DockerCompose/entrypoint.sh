#!/bin/bash

current_path=$(cd $(dirname $0);pwd)
start_what=$1

function on_stop(){
    echo "#### on_stop $start_what ####"
    if [[ "$start_what" != "confignode" ]]; then
        echo "###### manually flush ######";
        start-cli.sh -e "flush;" || true
        stop-datanode.sh
        echo "##### done ######";
    else
        stop-confignode.sh;
    fi
}

trap 'on_stop' SIGTERM SIGKILL SIGQUIT

replace-conf-from-env.sh ${start_what}

case "$1" in
   datanode)
       exec start-datanode.sh
       ;;
   confignode)
       exec start-confignode.sh
       ;;
   all)
       start-confignode.sh > /dev/null 2>&1 &
       sleep 5
       exec start-datanode.sh
       ;;
   *)
       echo "bad parameter!"
       exit -1
       ;;
esac
