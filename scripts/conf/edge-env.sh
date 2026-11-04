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

# IoTDB Edge runs ConfigNode and DataNode inside ONE JVM with a fixed, small
# memory budget so that it can share a machine with other processes.
# The defaults below target a total process RSS of about 512 MB and were
# validated on x86 servers and Raspberry Pi 4B class devices.

# On-heap memory of the merged process. Example values: '224M', '512M'.
ON_HEAP_MEMORY="${ON_HEAP_MEMORY:-224M}"
# Initial heap. Kept small so an idle edge instance stays light.
INIT_HEAP_MEMORY="${INIT_HEAP_MEMORY:-64M}"
# Off-heap (direct buffer) memory.
OFF_HEAP_MEMORY="${OFF_HEAP_MEMORY:-96M}"

if [ "${OFF_HEAP_MEMORY%"G"}" != "$OFF_HEAP_MEMORY" ]; then
    off_heap_memory_size_in_mb=$(expr ${OFF_HEAP_MEMORY%"G"} \* 1024)
else
    off_heap_memory_size_in_mb=$(expr ${OFF_HEAP_MEMORY%"M"})
fi
# Max cached buffer size, which equals OFF_HEAP_MEMORY / io threads number (200)
MAX_CACHED_BUFFER_SIZE=$(expr $off_heap_memory_size_in_mb \* 1024 \* 1024 / 200)

IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Diotdb.jmx.local=true"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xms${INIT_HEAP_MEMORY}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xmx${ON_HEAP_MEMORY}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:MaxDirectMemorySize=${OFF_HEAP_MEMORY}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Djdk.nio.maxCachedBufferSize=${MAX_CACHED_BUFFER_SIZE}"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+CrashOnOutOfMemoryError"
# Serial GC has the lowest fixed memory overhead; typical edge write rates leave
# plenty of latency headroom for its pauses.
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+UseSerialGC"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xss320k"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:MaxMetaspaceSize=160m"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:CompressedClassSpaceSize=40m"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:ReservedCodeCacheSize=64m"
# Cap the processors the JVM sees, shrinking internal thread pools on big hosts.
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:ActiveProcessorCount=2"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+UnlockDiagnosticVMOptions"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+UseCRC32Intrinsics"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:SafepointTimeoutDelay=1000"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+SafepointTimeout"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8"

# Append tsfile locale option populated by Maven at package time
# (see conf/iotdb-common.sh; empty in default build, "-Dtsfile.locale=zh" under with-zh-locale).
if [ -n "$TSFILE_LOCALE_JVM_OPT" ]; then
    IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS $TSFILE_LOCALE_JVM_OPT"
fi

echo "IoTDB Edge on heap memory size = ${ON_HEAP_MEMORY}B, off heap memory size = ${OFF_HEAP_MEMORY}B"
echo "If you want to change this configuration, please check conf/edge-env.sh."
