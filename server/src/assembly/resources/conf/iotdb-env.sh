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

# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME

calculate_heap_sizes()
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

    # set max heap size based on the following
    # max(min(1/2 ram, 1024MB), min(1/4 ram, 8GB))
    # calculate 1/2 ram and cap to 1024MB
    # calculate 1/4 ram and cap to 8192MB
    # pick the max
    half_system_memory_in_mb=`expr $system_memory_in_mb / 2`
    quarter_system_memory_in_mb=`expr $half_system_memory_in_mb / 2`
    if [ "$half_system_memory_in_mb" -gt "1024" ]
    then
        half_system_memory_in_mb="1024"
    fi
    if [ "$quarter_system_memory_in_mb" -gt "8192" ]
    then
        quarter_system_memory_in_mb="8192"
    fi
    if [ "$half_system_memory_in_mb" -gt "$quarter_system_memory_in_mb" ]
    then
        max_heap_size_in_mb="$half_system_memory_in_mb"
    else
        max_heap_size_in_mb="$quarter_system_memory_in_mb"
    fi
    MAX_HEAP_SIZE="${max_heap_size_in_mb}M"

    # Young gen: min(max_sensible_per_modern_cpu_core * num_cores, 1/4 * heap size)
    max_sensible_yg_per_core_in_mb="100"
    max_sensible_yg_in_mb=`expr $max_sensible_yg_per_core_in_mb "*" $system_cpu_cores`

    desired_yg_in_mb=`expr $max_heap_size_in_mb / 4`

    if [ "$desired_yg_in_mb" -gt "$max_sensible_yg_in_mb" ]
    then
        HEAP_NEWSIZE="${max_sensible_yg_in_mb}M"
    else
        HEAP_NEWSIZE="${desired_yg_in_mb}M"
    fi
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

#GC log path has to be defined here because it needs to access CASSANDRA_HOME
if [ "${version_arr[0]}" = "1" ] ; then
    # Java 8
    MAJOR_VERSION=${version_arr[1]}
    echo "$IOTDB_JMX_OPTS" | grep -q "^-[X]loggc"
    if [ "$?" = "1" ] ; then # [X] to prevent ccm from replacing this line
        # only add -Xlog:gc if it's not mentioned in jvm-server.options file
        mkdir -p ${IOTDB_HOME}/logs
        if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
            IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xloggc:${IOTDB_HOME}/logs/gc.log  -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M"
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
        fi
    fi
fi



calculate_heap_sizes

## Set heap size by percentage of total memory
#max_percentage=90
#min_percentage=50
#MAX_HEAP_SIZE="`expr $system_memory_in_mb \* $max_percentage / 100`M"
#HEAP_NEWSIZE="`expr $system_memory_in_mb \* $min_percentage / 100`M"

# Maximum heap size
#MAX_HEAP_SIZE="2G"
# Minimum heap size
#HEAP_NEWSIZE="2G"

#true or false
#DO NOT FORGET TO MODIFY THE PASSWORD FOR SECURITY (${IOTDB_CONF}/jmx.password and ${IOTDB_CONF}/jmx.access)
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
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=true"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.ssl=false"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=true"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.password.file=${IOTDB_CONF}/jmx.password"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.access.file=${IOTDB_CONF}/jmx.access"
  IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Djava.rmi.server.hostname=$JMX_IP"
else
  echo "setting local JMX..."
fi


IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xms${HEAP_NEWSIZE}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xmx${MAX_HEAP_SIZE}"

echo "Maximum memory allocation pool = ${MAX_HEAP_SIZE}B, initial memory allocation pool = ${HEAP_NEWSIZE}B"
echo "If you want to change this configuration, please check conf/iotdb-env.sh(Unix or OS X, if you use Windows, check conf/iotdb-env.bat)."

