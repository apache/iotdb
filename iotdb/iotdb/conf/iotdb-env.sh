#!/bin/sh
#
# Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


calculate_heap_sizes()
{
    case "`uname`" in
        Linux)
            system_memory_in_mb=`free -m | awk '/:/ {print $2;exit}'`
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


# Determine the sort of JVM we'll be running on.
java_ver_output=`"${JAVA:-java}" -version 2>&1`
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

calculate_heap_sizes

# Maximum heap size
#MAX_HEAP_SIZE="2G"
# Minimum heap size
#HEAP_NEWSIZE="2G"

LOCAL_JMX=no

JMX_PORT="31999"

if [ "$LOCAL_JMX" = "yes" ]; then
	IOTDB_JMX_OPTS="-Diotdb.jmx.local.port=$JMX_PORT"
	IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
else
	IOTDB_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false"
	IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

IOTDB_DERBY_OPTS="-Dderby.stream.error.field=org.apache.iotdb.db.auth.dao.DerbyUtil.DEV_NULL"

IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xloggc:${IOTDB_HOME}/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails"

IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xms${HEAP_NEWSIZE}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xmx${MAX_HEAP_SIZE}"
