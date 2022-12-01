#!/bin/bash

conf_path=${IOTDB_HOME}/conf
target_files="iotdb-common.properties iotdb-confignode.properties iotdb-datanode.properties"

function process_single(){
	local expect=$1
	local filename=$2
	line=$(grep -niG "^${expect%%=*}=" ${filename})
	#echo "line=$line"
	if [[ -n "${line}" ]]; then
		line_no=$(echo $line|cut -d : -f1)
		content=$(echo $line|cut -d : -f2)
		if [[ "${content:0:1}" != "#" ]]; then
		       #sed -i "s|${content}|#${content}|g" ${filename} 
		       sed -i "${line_no}d" ${filename}

	        fi
                sed -i "${line_no} i${expect}" ${filename}
	fi
}

function replace_configs(){
  for v in $(env); do
    if [[ "${v}" =~ "=" && "${v}" =~ "_" && ! "${v}" =~ "JAVA_" ]]; then
      #echo "###### $v ####"
      for f in ${target_files}; do
	      process_single $v ${conf_path}/$f
      done
    fi
  done
}

replace_configs

