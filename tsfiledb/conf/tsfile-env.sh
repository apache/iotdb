#!/bin/sh

#export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_112.jdk/Contents/Home

LOCAL_JMX=yes

JMX_PORT="31999"

if [ "$LOCAL_JMX" = "yes" ]; then
	TSFILEDB_JMX_OPTS="-Dtsfiledb.jmx.local.port=$JMX_PORT"
else
	TSFILEDB_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false"
	TSFILEDB_JMX_OPTS="$TSFILEDB_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi