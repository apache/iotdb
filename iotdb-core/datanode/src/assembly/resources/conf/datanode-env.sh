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

# You can set DataNode memory size, example '2G' or '2048M'
MEMORY_SIZE=

# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME

# Set max number of open files
max_num=$(ulimit -n)
if [ $max_num -le 65535 ]; then
    ulimit -n 65535
    if [ $? -ne 0 ]; then
        echo "Warning: Failed to set max number of files to be 65535, maybe you need to use 'sudo ulimit -n 65535' to set it when you use iotdb in production environments."
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

calculate_memory_sizes()
{
    case "`uname`" in
        Linux)
            system_memory_in_mb=`free -m| sed -n '2p' | awk '{print $2}'`
            system_cpu_cores=`egrep -c 'processor([[:space:]]+):.*' /proc/cpuinfo`
        ;;
        FreeBSD)
            system_memory_in_bytes=`sysctl hw.physmem | awk '{print $2}'`
            system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
            system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
        ;;
        SunOS)
            system_memory_in_mb=`prtconf | awk '/Memory size:/ {print $3}'`
            system_cpu_cores=`psrinfo | wc -l`
        ;;
        Darwin)
            system_memory_in_bytes=`sysctl hw.memsize | awk '{print $2}'`
            system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
            system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
        ;;
        *)
            # assume reasonable defaults for e.g. a modern desktop or
            # cheap server
            system_memory_in_mb="2048"
            system_cpu_cores="2"
        ;;
    esac

    # some systems like the raspberry pi don't report cores, use at least 1
    if [ "$system_cpu_cores" -lt "1" ]
    then
        system_cpu_cores="1"
    fi

    # suggest using memory, system memory 1 / 2
    suggest_using_memory_in_mb=`expr $system_memory_in_mb / 2`

    if [ -n "$MEMORY_SIZE" ]
    then
        if [ "${MEMORY_SIZE%"G"}" != "$MEMORY_SIZE" ] || [ "${MEMORY_SIZE%"M"}" != "$MEMORY_SIZE" ]
        then
          if [ "${MEMORY_SIZE%"G"}" != "$MEMORY_SIZE" ]
          then
              memory_size_in_mb=`expr ${MEMORY_SIZE%"G"} "*" 1024`
          else
              memory_size_in_mb=`expr ${MEMORY_SIZE%"M"}`
          fi
        else
            echo "Invalid format of MEMORY_SIZE, please use the format like 2048M or 2G"
            exit 1
        fi
    else
        memory_size_in_mb=$suggest_using_memory_in_mb
    fi

    # set on heap memory size
    # when memory_size_in_mb is less than 4 * 1024, we will set on heap memory size to memory_size_in_mb / 4 * 3
    # when memory_size_in_mb is greater than 4 * 1024 and less than 16 * 1024, we will set on heap memory size to memory_size_in_mb / 5 * 4
    # when memory_size_in_mb is greater than 16 * 1024 and less than 128 * 1024, we will set on heap memory size to memory_size_in_mb / 8 * 7
    # when memory_size_in_mb is greater than 128 * 1024, we will set on heap memory size to memory_size_in_mb - 16 * 1024
    if [ "$memory_size_in_mb" -lt "4096" ]
    then
        on_heap_memory_size_in_mb=`expr $memory_size_in_mb / 4 \* 3`
    elif [ "$memory_size_in_mb" -lt "16384" ]
    then
        on_heap_memory_size_in_mb=`expr $memory_size_in_mb / 5 \* 4`
    elif [ "$memory_size_in_mb" -lt "131072" ]
    then
        on_heap_memory_size_in_mb=`expr $memory_size_in_mb / 8 \* 7`
    else
        on_heap_memory_size_in_mb=`expr $memory_size_in_mb - 16384`
    fi
    off_heap_memory_size_in_mb=`expr $memory_size_in_mb - $on_heap_memory_size_in_mb`

    ON_HEAP_MEMORY="${on_heap_memory_size_in_mb}M"
    OFF_HEAP_MEMORY="${off_heap_memory_size_in_mb}M"
}


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

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
    exit 1;
fi

# Determine the sort of JVM we'll be running on.
java_ver_output=`"$JAVA" -version 2>&1`
jvmver=`echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}' | cut -d\- -f1`
JVM_VERSION=${jvmver%_*}
JVM_PATCH_VERSION=${jvmver#*_}
if [ "$JVM_VERSION" \< "1.8" ] ; then
    echo "IoTDB requires Java 8u40 or later."
    exit 1;
fi

if [ "$JVM_VERSION" \< "1.8" ] && [ "$JVM_PATCH_VERSION" -lt 40 ] ; then
    echo "IoTDB requires Java 8u40 or later."
    exit 1;
fi

version_arr=(${JVM_VERSION//./ })

illegal_access_params=""
#GC log path has to be defined here because it needs to access IOTDB_HOME
if [ "${version_arr[0]}" = "1" ] ; then
    # Java 8
    MAJOR_VERSION=${version_arr[1]}
    echo "$IOTDB_JMX_OPTS" | grep -q "^-[X]loggc"
    if [ "$?" = "1" ] ; then # [X] to prevent ccm from replacing this line
        # only add -Xlog:gc if it's not mentioned in jvm-server.options file
        mkdir -p ${IOTDB_HOME}/logs
        if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
            IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xloggc:${IOTDB_HOME}/logs/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M"
            # For more detailed GC information, you can uncomment option below.
            # NOTE: more detailed GC information may bring larger GC log files.
            # IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xloggc:${IOTDB_HOME}/logs/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+UseGCLogFileRotation -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC -XX:+PrintReferenceGC -XX:+PrintSafepointStatistics -XX:PrintSafepointStatisticsCount=1 -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
        fi
    fi
else
    #JDK 11 and others
    MAJOR_VERSION=${version_arr[0]}
    # See description of https://bugs.openjdk.java.net/browse/JDK-8046148 for details about the syntax
    # The following is the equivalent to -XX:+PrintGCDetails -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M
    echo "$IOTDB_JMX_OPTS" | grep -q "^-[X]log:gc"
    if [ "$?" = "1" ] ; then # [X] to prevent ccm from replacing this line
        # only add -Xlog:gc if it's not mentioned in jvm-server.options file
        mkdir -p ${IOTDB_HOME}/logs
        if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
            IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xlog:gc=info,heap*=info,age*=info,safepoint=info,promotion*=info:file=${IOTDB_HOME}/logs/gc.log:time,uptime,pid,tid,level:filecount=10,filesize=10485760"
            # For more detailed GC information, you can uncomment option below.
            # NOTE: more detailed GC information may bring larger GC log files.
            # IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xlog:gc*=debug,heap*=debug,age*=trace,metaspace*=info,safepoint*=debug,promotion*=info:file=${IOTDB_HOME}/logs/gc.log:time,uptime,pid,tid,level,tags:filecount=10,filesize=100M"
        fi
    fi
    # Add argLine for Java 11 and above, due to [JEP 396: Strongly Encapsulate JDK Internals by Default] (https://openjdk.java.net/jeps/396)
    illegal_access_params="$illegal_access_params --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
    illegal_access_params="$illegal_access_params --add-opens=java.base/java.lang=ALL-UNNAMED"
    illegal_access_params="$illegal_access_params --add-opens=java.base/java.util=ALL-UNNAMED"
    illegal_access_params="$illegal_access_params --add-opens=java.base/java.nio=ALL-UNNAMED"
    illegal_access_params="$illegal_access_params --add-opens=java.base/java.io=ALL-UNNAMED"
    illegal_access_params="$illegal_access_params --add-opens=java.base/java.net=ALL-UNNAMED"
fi


calculate_memory_sizes

# on heap memory size
#ON_HEAP_MEMORY="2G"
# off heap memory size
#OFF_HEAP_MEMORY="512M"


if [ "${OFF_HEAP_MEMORY%"G"}" != "$OFF_HEAP_MEMORY" ]
then
    off_heap_memory_size_in_mb=`expr ${OFF_HEAP_MEMORY%"G"} "*" 1024`
else
    off_heap_memory_size_in_mb=`expr ${OFF_HEAP_MEMORY%"M"}`
fi

# threads number for io
IO_THREADS_NUMBER="1000"
# Max cached buffer size, Note: unit can only be B!
# which equals OFF_HEAP_MEMORY / IO_THREADS_NUMBER 
MAX_CACHED_BUFFER_SIZE=`expr $off_heap_memory_size_in_mb \* 1024 \* 1024 / $IO_THREADS_NUMBER`

#true or false
#DO NOT FORGET TO MODIFY THE PASSWORD FOR SECURITY (${IOTDB_CONF}/jmx.password and ${IOTDB_CONF}/jmx.access)
#If you want to connect JMX Service by network in local machine, such as nodeTool.sh will try to connect 127.0.0.1:31999, please set JMX_LOCAL to false.
JMX_LOCAL="true"

JMX_PORT="31999"
#only take effect when the jmx_local=false
#You need to change this IP as a public IP if you want to remotely connect IoTDB by JMX.
# 0.0.0.0 is not allowed
JMX_IP="127.0.0.1"

if [ ${JMX_LOCAL} = "false" ]; then
  echo "setting remote JMX..."
  #you may have no permission to run chmod. If so, contact your system administrator.
  chmod 600 ${IOTDB_CONF}/jmx.password
  chmod 600 ${IOTDB_CONF}/jmx.access
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Djava.rmi.server.randomIDs=true"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.ssl=false"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=true"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.password.file=${IOTDB_CONF}/jmx.password"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.access.file=${IOTDB_CONF}/jmx.access"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Djava.rmi.server.hostname=$JMX_IP"
else
  echo "setting local JMX..."
fi

IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Diotdb.jmx.local=$JMX_LOCAL"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xms${ON_HEAP_MEMORY}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xmx${ON_HEAP_MEMORY}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:MaxDirectMemorySize=${OFF_HEAP_MEMORY}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Djdk.nio.maxCachedBufferSize=${MAX_CACHED_BUFFER_SIZE}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+CrashOnOutOfMemoryError"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+UseAdaptiveSizePolicy"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xss512k"
# these two options print safepoints with pauses longer than 1000ms to the standard output. You can see these logs via redirection when starting in the background like "start-datanode.sh > log_datanode_safepoint.log"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:SafepointTimeoutDelay=1000"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+SafepointTimeout"

# option below tries to optimize safepoint stw time for large counted loop.
# NOTE: it may have an impact on JIT's black-box optimization.
# IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+UseCountedLoopSafepoints"

# when the GC time is too long, if there are remaining CPU resources, you can try to turn on and increase options below.
# for Linux:
# CPU_PROCESSOR_NUM=$(nproc)
# for MacOS:
# CPU_PROCESSOR_NUM=$(sysctl -n hw.ncpu)
# IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:ParallelGCThreads=${CPU_PROCESSOR_NUM}"

# if there are much of stw time of reference process in GC log, you can turn on option below.
# NOTE: it may have an impact on application's throughput.
# IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+ParallelRefProcEnabled"

# this option can reduce the overhead caused by memory allocation, page fault interrupts, etc. during JVM operation.
# NOTE: it may reduce memory utilization and trigger OOM killer when memory is tight.
# IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+AlwaysPreTouch"

# if you want to dump the heap memory while OOM happening, you can use the following command, remember to replace /tmp/heapdump.hprof with your own file path and the folder where this file is located needs to be created in advance
# IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/datanode_heapdump.hprof"


echo "DataNode on heap memory size = ${ON_HEAP_MEMORY}B, off heap memory size = ${OFF_HEAP_MEMORY}B"
echo "If you want to change this configuration, please check conf/datanode-env.sh."

