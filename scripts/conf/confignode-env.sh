#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# You can set ConfigNode memory size, example '2G' or '2048M'
# If the MEMORY_SIZE environment variable is already set, its value will be used.
MEMORY_SIZE=${MEMORY_SIZE:-}

# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME

# Set max number of open files
max_num=$(ulimit -n)
if [ $max_num -le 65535 ]; then
    ulimit -n 65535
    if [ $? -ne 0 ]; then
        echo "Warning: Failed to set max number of files to be 65535, maybe you need to use 'sudo ulimit -n 65535' to set it when you use iotdb ConfigNode in production environments."
    fi
fi

# Set somaxconn to a better value to avoid meaningless connection reset issues when the system is under high load.
# The original somaxconn will be set back when the system reboots.
# For more detail, see: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=19f92a030ca6d772ab44b22ee6a01378a8cb32d4
SOMAXCONN=65535
case "$(uname)" in
    Linux)
        somaxconn=$(sysctl -n net.core.somaxconn)
        if [ "$somaxconn" -lt $SOMAXCONN ]; then
            echo "WARN:"
            echo "WARN: the value of net.core.somaxconn (=$somaxconn) is too small, please set it to a larger value using the following command."
            echo "WARN:     sudo sysctl -w net.core.somaxconn=$SOMAXCONN"
            echo "WARN: The original net.core.somaxconn value will be set back when the os reboots."
            echo "WARN:"
        fi
    ;;
    FreeBSD | Darwin)
        somaxconn=$(sysctl -n kern.ipc.somaxconn)
        if [ "$somaxconn" -lt $SOMAXCONN ]; then
            echo "WARN:"
            echo "WARN: the value of kern.ipc.somaxconn (=$somaxconn) is too small, please set it to a larger value using the following command."
            echo "WARN:     sudo sysctl -w kern.ipc.somaxconn=$SOMAXCONN"
            echo "WARN: The original kern.ipc.somaxconn value will be set back when the os reboots."
            echo "WARN:"
        fi
    ;;
esac

# whether we allow enable heap dump files
IOTDB_ALLOW_HEAP_DUMP="true"

get_cn_system_dir() {
    local config_file="$1"
    local cn_system_dir=""

    cn_system_dir=`sed '/^cn_system_dir=/!d;s/.*=//' ${CONFIGNODE_CONF}/${config_file} | tail -n 1`

    if [ -z "$cn_system_dir" ]; then
        echo ""
        return 0
    fi

    if [[ "$cn_system_dir" == /* ]]; then
        echo "$cn_system_dir"
    else
        echo "$CONFIGNODE_HOME/$cn_system_dir"
    fi
}

if [ -f "${CONFIGNODE_CONF}/iotdb-system.properties" ]; then
  	heap_dump_dir=$(get_cn_system_dir "iotdb-system.properties")
else
  	heap_dump_dir=$(get_cn_system_dir "iotdb-confignode.properties")
fi

if [ -z "$heap_dump_dir" ]; then
  	heap_dump_dir="$CONFIGNODE_HOME/data/confignode/system"
fi

if [ ! -d "$heap_dump_dir" ]; then
  	mkdir -p "$heap_dump_dir"
fi

# find java in JAVA_HOME
if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

if [ -z "$JAVA" ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
    exit 1;
fi

# Determine the sort of JVM we'll be running on.
java_ver_output=`"$JAVA" -version 2>&1`
jvmver=`echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}' | cut -d\- -f1`
JVM_VERSION=${jvmver%_*}
version_arr=(${JVM_VERSION//./ })
if [ "${version_arr[0]}" = "1" ] ; then
    MAJOR_VERSION=${version_arr[1]}
else
    MAJOR_VERSION=${version_arr[0]}
fi

if [ "$MAJOR_VERSION" -lt 17 ] ; then
    echo "IoTDB requires Java 17 or later."
    exit 1;
fi

illegal_access_params=""
#GC log path has to be defined here because it needs to access CONFIGNODE_HOME
# See description of https://bugs.openjdk.java.net/browse/JDK-8046148 for details about the syntax
# The following is the equivalent to -XX:+PrintGCDetails -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M
echo "$CONFIGNODE_JMX_OPTS" | grep -q "^-[X]log:gc"
if [ "$?" = "1" ] ; then # [X] to prevent ccm from replacing this line
    # only add -Xlog:gc if it's not mentioned in jvm-server.options file
    mkdir -p ${CONFIGNODE_HOME}/logs
    if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
        CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Xlog:gc=info,heap*=info,age*=info,safepoint=info,promotion*=info:file=${CONFIGNODE_HOME}/logs/gc.log:time,uptime,pid,tid,level:filecount=10,filesize=10485760"
    fi
fi
# Add argLine for Java 17 and above, due to [JEP 396: Strongly Encapsulate JDK Internals by Default] (https://openjdk.java.net/jeps/396)
illegal_access_params="$illegal_access_params --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.lang=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.util=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.nio=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.io=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.net=ALL-UNNAMED"


# ConfigNode: suggest 30% of system memory (3/10), cap at 8 GB.
calculate_memory_sizes 3 10 8192

# on heap memory size
#ON_HEAP_MEMORY="2G"
# off heap memory size
#OFF_HEAP_MEMORY="512M"

# configure JVM memory with setting environment variable of CONFIGNODE_JMX_OPTS
if [[ "$CONFIGNODE_JMX_OPTS" =~ -Xmx ]];then
    item_arr=(${CONFIGNODE_JMX_OPTS})
    for item in ${item_arr[@]};do
        if [[ -n "$item" ]]; then
            if [[ "$item" =~ -Xmx ]]; then
                ON_HEAP_MEMORY=${item#*mx}
            elif [[ "$item" =~ -XX:MaxDirectMemorySize= ]]; then
                OFF_HEAP_MEMORY=${item#*=}
            fi
        fi
    done
fi


if [ "${OFF_HEAP_MEMORY%"G"}" != "$OFF_HEAP_MEMORY" ]
then
    off_heap_memory_size_in_mb=`expr ${OFF_HEAP_MEMORY%"G"} "*" 1024`
else
    off_heap_memory_size_in_mb=`expr ${OFF_HEAP_MEMORY%"M"}`
fi

# threads number of io
IO_THREADS_NUMBER="100"
# Max cached buffer size, Note: unit can only be B!
# which equals OFF_HEAP_MEMORY / IO_THREADS_NUMBER 
MAX_CACHED_BUFFER_SIZE=`expr $off_heap_memory_size_in_mb \* 1024 \* 1024 / $IO_THREADS_NUMBER`

#true or false
#DO NOT FORGET TO MODIFY THE PASSWORD FOR SECURITY (${CONFIGNODE_CONF}/jmx.password and ${CONFIGNODE_CONF}/jmx.access)
#If you want to connect JMX Service by network in local machine, such as nodeTool.sh will try to connect 127.0.0.1:31999, please set JMX_LOCAL to false.
JMX_LOCAL="true"

JMX_PORT="32000"
#only take effect when the jmx_local=false
#You need to change this IP as a public IP if you want to remotely connect IoTDB ConfigNode by JMX.
# 0.0.0.0 is not allowed
JMX_IP="127.0.0.1"

if [ ${JMX_LOCAL} = "false" ]; then
  echo "setting remote JMX..."
  #you may have no permission to run chmod. If so, contact your system administrator.
  chmod 600 ${CONFIGNODE_CONF}/jmx.password
  chmod 600 ${CONFIGNODE_CONF}/jmx.access
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dcom.sun.management.jmxremote"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Djava.rmi.server.randomIDs=true"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dcom.sun.management.jmxremote.ssl=false"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=true"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dcom.sun.management.jmxremote.password.file=${CONFIGNODE_CONF}/jmx.password"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dcom.sun.management.jmxremote.access.file=${CONFIGNODE_CONF}/jmx.access"
  CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Djava.rmi.server.hostname=$JMX_IP"
else
  echo "setting local JMX..."
fi

CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Diotdb.jmx.local=$JMX_LOCAL"
if [[ ! "$CONFIGNODE_JMX_OPTS" =~ -Xms ]]; then CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Xms${ON_HEAP_MEMORY}"; fi
if [[ ! "$CONFIGNODE_JMX_OPTS" =~ -Xmx ]]; then CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Xmx${ON_HEAP_MEMORY}"; fi
if [[ ! "$CONFIGNODE_JMX_OPTS" =~ -XX:MaxDirectMemorySize ]]; then CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -XX:MaxDirectMemorySize=${OFF_HEAP_MEMORY}"; fi
CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Djdk.nio.maxCachedBufferSize=${MAX_CACHED_BUFFER_SIZE}"
CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -XX:+CrashOnOutOfMemoryError"
CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8"

# Append tsfile locale option populated by Maven at package time
# (see conf/iotdb-common.sh; empty in default build, "-Dtsfile.locale=zh" under with-zh-locale).
if [ -n "$TSFILE_LOCALE_JVM_OPT" ]; then
    CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS $TSFILE_LOCALE_JVM_OPT"
fi

# if you want to dump the heap memory while OOM happening, you can use the following command, remember to replace ${heap_dump_dir}/confignode_heapdump.hprof with your own file path and the folder where this file is located needs to be created in advance
#CONFIGNODE_JMX_OPTS="$CONFIGNODE_JMX_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${heap_dump_dir}/confignode_heapdump.hprof"

echo "ConfigNode on heap memory size = ${ON_HEAP_MEMORY}B, off heap memory size = ${OFF_HEAP_MEMORY}B"
echo "If you want to change this configuration, please check conf/confignode-env.sh."
