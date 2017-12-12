#!/bin/sh

LOCAL_JMX=no

JMX_PORT="31999"

if [ "$LOCAL_JMX" = "yes" ]; then
	IOTDB_JMX_OPTS="-Diotdb.jmx.local.port=$JMX_PORT"
	IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
else
	IOTDB_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false"
	IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

IOTDB_DERBY_OPTS="-Dderby.stream.error.field=cn.edu.tsinghua.iotdb.auth.dao.DerbyUtil.DEV_NULL"

IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xloggc:${IOTDB_HOME}/logs/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xms2G"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xmx2G"
